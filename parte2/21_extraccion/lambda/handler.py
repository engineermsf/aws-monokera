import logging
import os

from extraction.run import run

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    output_path = event.get("output_path") or os.environ.get("BRONZE_OUTPUT_PATH", "/tmp/bronze")
    result = run(output_path)
    return {"statusCode": 200, "body": result}
