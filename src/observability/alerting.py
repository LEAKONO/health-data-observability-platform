"""
src/observability/alerting.py

Sends a Slack notification when a pipeline stage fails. This is
the "someone gets notified immediately" piece of observability --
without it, a failure just sits quietly in PIPELINE_LOGS until
someone happens to look.
"""

import os

import requests

from src.observability.logger_config import get_logger

logger = get_logger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def send_failure_alert(run_id: str, pipeline_name: str, stage: str, error_message: str) -> bool:
   
    if not SLACK_WEBHOOK_URL:
        logger.info(
            "No Slack webhook configured -- skipping alert",
            extra={"run_id": run_id, "stage": stage},
        )
        return False

    message = {
        "text": (
            f":rotating_light: *Pipeline Failure*\n"
            f"*Pipeline:* {pipeline_name}\n"
            f"*Stage:* {stage}\n"
            f"*Run ID:* {run_id}\n"
            f"*Error:* {error_message}"
        )
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
        response.raise_for_status()
        logger.info(
            "Slack alert sent successfully",
            extra={"run_id": run_id, "stage": stage},
        )
        return True
    except requests.exceptions.RequestException as e:
        logger.error(
            "Failed to send Slack alert",
            extra={"run_id": run_id, "stage": stage, "error_message": str(e)},
        )
        return False


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    result = send_failure_alert(
        run_id="test_run_001",
        pipeline_name="covid_deaths_pipeline",
        stage="load",
        error_message="This is a test alert to confirm Slack integration works.",
    )
    print(f"Alert sent: {result}")