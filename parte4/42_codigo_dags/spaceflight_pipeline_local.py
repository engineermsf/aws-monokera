"""
DAG del pipeline Spaceflight: extracción (Lambda) → Silver → Gold (Glue en Docker).
- Extracción: invoca la Lambda con la conexión aws_default. El límite por tipo (extraction_max_items)
  se puede pasar al Trigger DAG (conf) o por Variable; si va vacío, la Lambda extrae todo.
- Silver/Gold: mismo aws_default; las credenciales se pasan al contenedor Glue desde la conexión.
Requiere:
  - Conexión aws_default (p. ej. en .env como AIRFLOW_CONN_AWS_DEFAULT): acceso Lambda + credenciales para Glue.
  - Variables (sin valores por defecto en código): bronze_path, silver_warehouse, gold_warehouse, glue_image,
    repo_host_path, lambda_function_name (opcional, por defecto monokera-extraccion).
"""
import json
from datetime import datetime

from docker.types import Mount
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

# Variables leídas en parse time: los params del DockerOperator no se templatean dentro del contenedor,
# así que hay que pasar valores ya resueltos (no strings "{{ var.value.xxx }}").
REPO_HOST_PATH = Variable.get("repo_host_path", default_var="/home/hadoop/workspace")
BRONZE_PATH = Variable.get("bronze_path", default_var="")
SILVER_WAREHOUSE = Variable.get("silver_warehouse", default_var="")
GOLD_WAREHOUSE = Variable.get("gold_warehouse", default_var="")
GLUE_IMAGE = Variable.get("glue_image", default_var="")
PIPELINE_MOUNTS = [Mount(source=REPO_HOST_PATH, target="/home/hadoop/workspace", type="bind")]
LAMBDA_FUNCTION = Variable.get("lambda_function_name", default_var="monokera-extraccion")


def _docker_env():
    """Credenciales para el contenedor Glue: se reutilizan desde la conexión aws_default."""
    env = {"AWS_PROFILE": Variable.get("aws_profile", default_var="default")}
    try:
        conn = BaseHook.get_connection("aws_default")
        if getattr(conn, "login", None):
            env["AWS_ACCESS_KEY_ID"] = conn.login
        if getattr(conn, "password", None):
            env["AWS_SECRET_ACCESS_KEY"] = conn.password
        extra = getattr(conn, "extra_dejson", None) or {}
        if isinstance(extra, str):
            extra = json.loads(extra) if extra else {}
        if extra.get("aws_session_token"):
            env["AWS_SESSION_TOKEN"] = extra["aws_session_token"]
        if extra.get("region_name"):
            env["AWS_DEFAULT_REGION"] = extra["region_name"]
    except Exception:
        pass
    return env


def _log_extraction_payload(**context):
    """Log del valor extraction_max_items que se inyectará en el payload a la Lambda."""
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
            "Payload a Lambda: extraction_max_items = %r (conf keys extraction*= %r, params.extraction_max_items=%r)",
            val,
            {k: v for k, v in conf.items() if "extraction" in k.lower() or k == "params"},
            params.get("extraction_max_items"),
        )
    else:
        import logging
        logging.getLogger(__name__).info(
            "Payload a Lambda: extraction_max_items = %r",
            val,
        )


with DAG(
    dag_id="spaceflight_pipeline_local",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spaceflight", "bronze", "silver", "gold"],
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
        # Payload: extraction_max_items desde conf (varias claves por cómo puede llegar del form) > params > Variable.
        # Si no llega "all" desde Airflow, la Lambda usará EXTRACCION_MAX_ITEMS (env); revisar logs del task "extract" para ver el payload.
        payload='{"output_path": "{{ var.value.bronze_path }}", "ingestion_date": "{{ ds }}", "extraction_max_items": "{{ (dag_run.conf.get(\'extraction_max_items\') or dag_run.conf.get(\'extractionMaxItems\') or (dag_run.conf.get(\'params\') or {}).get(\'extraction_max_items\') or params.extraction_max_items or (var.value.extraction_max_items | default(\'\'))) or \'\' }}"}',
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        # Si la extracción es completa (~6–7 min), la conexión aws_default debe tener en Extra:
        # "config_kwargs": {"read_timeout": 900, "connect_timeout": 10} para evitar Read timeout.
    )

    silver = DockerOperator(
        task_id="silver",
        image=GLUE_IMAGE,
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=PIPELINE_MOUNTS,
        environment=_docker_env(),
        command=[
            "spark-submit",
            "--conf", "spark.driver.memory=4g",
            "--conf", "spark.executor.memory=3g",
            "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg.type=hadoop",
            "--conf", "spark.sql.catalog.iceberg.warehouse={{ params.silver_warehouse }}",
            "/home/hadoop/workspace/parte2/22_procesamiento/jobs/clean_and_deduplicate.py",
            "--bronze_path", "{{ params.bronze_path }}",
            "--silver_warehouse", "{{ params.silver_warehouse }}",
            "--partition_date", "{{ ds }}",
        ],
        params={
            "bronze_path": BRONZE_PATH,
            "silver_warehouse": SILVER_WAREHOUSE,
        },
    )

    gold = DockerOperator(
        task_id="gold",
        image=GLUE_IMAGE,
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=PIPELINE_MOUNTS,
        environment=_docker_env(),
        command=[
            "spark-submit",
            "--conf", "spark.driver.memory=4g",
            "--conf", "spark.executor.memory=3g",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--conf", "spark.sql.shuffle.partitions=8",
            "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.iceberg.type=hadoop",
            "--conf", "spark.sql.catalog.iceberg.warehouse={{ params.gold_warehouse }}",
            "/home/hadoop/workspace/parte2/22_procesamiento/jobs/content_and_gold.py",
            "--silver_warehouse", "{{ params.silver_warehouse }}",
            "--gold_warehouse", "{{ params.gold_warehouse }}",
        ],
        params={
            "silver_warehouse": SILVER_WAREHOUSE,
            "gold_warehouse": GOLD_WAREHOUSE,
        },
    )

    log_payload >> extract >> silver >> gold
