"""
DAG del pipeline Spaceflight ejecutado íntegramente en AWS:
  log_payload → extract (Lambda) → silver (Glue job en AWS) → gold (Glue job en AWS).
Misma estrategia que spaceflight_pipeline_local: params extraction_max_items, payload Lambda con
output_path, ingestion_date y extraction_max_items (conf/params/Variable), rutas desde Variables.

Requiere:
  - Conexión aws_default (AIRFLOW_CONN_AWS_DEFAULT con region_name y token si SSO).
  - Variables opcionales:
    - bronze_path, silver_warehouse, gold_warehouse (rutas S3; defaults abajo)
    - lambda_function_name (default: monokera-extraccion)
    - glue_job_silver, glue_job_gold (nombres de los jobs Glue)
    - extraction_max_items (opcional; o pasarlo en Trigger)
  - Los jobs Glue deben existir en AWS y tener los scripts en S3 (ver docs de deploy).
"""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

# Región y rutas por defecto cuando no están definidas las Variables de Airflow (entorno dev).
# En otros entornos conviene definir bronze_path, silver_warehouse, gold_warehouse en Airflow.
AWS_REGION = "us-east-1"
DEFAULT_BRONZE = "s3://bnc-lkh-dev-bronze/spaceflight"
DEFAULT_SILVER = "s3://bnc-lkh-dev-silver/warehouse"
DEFAULT_GOLD = "s3://bnc-lkh-dev-gold/warehouse"

BRONZE_PATH = Variable.get("bronze_path", default_var=DEFAULT_BRONZE)
SILVER_WAREHOUSE = Variable.get("silver_warehouse", default_var=DEFAULT_SILVER)
GOLD_WAREHOUSE = Variable.get("gold_warehouse", default_var=DEFAULT_GOLD)
LAMBDA_FUNCTION = Variable.get("lambda_function_name", default_var="monokera-extraccion")
GLUE_JOB_SILVER = Variable.get("glue_job_silver", default_var="monokera-spaceflight-silver")
GLUE_JOB_GOLD = Variable.get("glue_job_gold", default_var="monokera-spaceflight-gold")


def _log_extraction_payload(**context):
    """Log del valor extraction_max_items que se inyectará en el payload a la Lambda (mismo criterio que pipeline local)."""
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    if not isinstance(conf, dict):
        conf = {}
    params = context.get("params") or {}
    nested = (conf.get("params") or {}) if isinstance(conf.get("params"), dict) else {}
    val = (
        conf.get("extraction_max_items")
        or conf.get("extractionMaxItems")
        or nested.get("extraction_max_items")
        or params.get("extraction_max_items")
        or Variable.get("extraction_max_items", default_var="")
        or ""
    )
    log = context.get("log")
    if log:
        log.info(
            "Payload a Lambda: extraction_max_items = %r",
            val,
        )
    else:
        import logging
        logging.getLogger(__name__).info("Payload a Lambda: extraction_max_items = %r", val)


with DAG(
    dag_id="spaceflight_pipeline_aws",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spaceflight", "aws", "bronze", "silver", "gold"],
    params={
        "extraction_max_items": Param(
            default=None,
            type=["string", "null"],
            description="Opcional. Número (ej. 1) para limitar, o 'all' / vacío para extraer todo.",
        ),
    },
) as dag:
    log_payload = PythonOperator(
        task_id="log_payload",
        python_callable=_log_extraction_payload,
    )
    extract = LambdaInvokeFunctionOperator(
        task_id="extract",
        function_name=LAMBDA_FUNCTION,
        payload='{"output_path": "{{ var.value.bronze_path | default(\'s3://bnc-lkh-dev-bronze/spaceflight\') }}", "ingestion_date": "{{ ds }}", "extraction_max_items": "{{ (dag_run.conf.get(\'extraction_max_items\') or dag_run.conf.get(\'extractionMaxItems\') or (dag_run.conf.get(\'params\') or {}).get(\'extraction_max_items\') or params.extraction_max_items or (var.value.extraction_max_items | default(\'\'))) or \'\' }}"}',
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        # Extracción completa (~6–7 min): en aws_default poner "config_kwargs": {"read_timeout": 900, "connect_timeout": 10}.
    )

    silver = GlueJobOperator(
        task_id="silver",
        job_name=GLUE_JOB_SILVER,
        script_args={
            "--bronze_path": "{{ var.value.bronze_path | default('s3://bnc-lkh-dev-bronze/spaceflight') }}",
            "--silver_warehouse": "{{ var.value.silver_warehouse | default('s3://bnc-lkh-dev-silver/warehouse') }}",
            "--partition_date": "{{ ds }}",
            "--catalog_silver": "glue",
        },
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=True,
    )

    gold = GlueJobOperator(
        task_id="gold",
        job_name=GLUE_JOB_GOLD,
        script_args={
            "--silver_warehouse": "{{ var.value.silver_warehouse | default('s3://bnc-lkh-dev-silver/warehouse') }}",
            "--gold_warehouse": "{{ var.value.gold_warehouse | default('s3://bnc-lkh-dev-gold/warehouse') }}",
            "--catalog_silver": "glue",
            "--catalog_gold": "glue",
        },
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=True,
    )

    log_payload >> extract >> silver >> gold
