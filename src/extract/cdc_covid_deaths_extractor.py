import time
import uuid
from datetime import datetime, timezone

import requests

from src.utils.snowflake_connection import get_snowflake_connection
from src.observability.logger_config import get_logger

logger = get_logger(__name__)

CDC_API_URL = "https://data.cdc.gov/resource/r8kw-7aab.json"
SOURCE_NAME = "covid_deaths"


def get_last_snapshot_date(conn) -> str | None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT MAX(snapshot_date) FROM RAW.COVID_DEATHS_RAW WHERE state = 'United States'"
        )
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    finally:
        cursor.close()


def fetch_new_covid_data(last_snapshot_date: str | None) -> list[dict]:
    params = {
        "$order": "end_week ASC",
        "$limit": 5000,
    }
    if last_snapshot_date:
        params["$where"] = f"end_week > '{last_snapshot_date}'"

    response = requests.get(CDC_API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def transform_record(record: dict, run_id: str) -> dict:
    return {
        "run_id": run_id,
        "snapshot_date": record.get("end_week", "")[:10],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "state": record.get("state"),
        "covid_19_deaths": int(record["covid_19_deaths"]) if record.get("covid_19_deaths") else None,
        "total_deaths": int(record["total_deaths"]) if record.get("total_deaths") else None,
        "pneumonia_deaths": int(record["pneumonia_deaths"]) if record.get("pneumonia_deaths") else None,
        "pneumonia_and_covid_19_deaths": int(record["pneumonia_and_covid_19_deaths"]) if record.get("pneumonia_and_covid_19_deaths") else None,
        "influenza_deaths": int(record["influenza_deaths"]) if record.get("influenza_deaths") else None,
        "raw_payload": record,
    }


def run_extraction() -> list[dict]:
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    start_time = time.time()

    logger.info(
        "Extraction started",
        extra={"run_id": run_id, "stage": "extract", "source": SOURCE_NAME},
    )

    conn = get_snowflake_connection()
    try:
        last_snapshot_date = get_last_snapshot_date(conn)
        logger.info(
            "Checked last snapshot date",
            extra={"run_id": run_id, "last_snapshot_date": str(last_snapshot_date)},
        )

        raw_records = fetch_new_covid_data(last_snapshot_date)

        if not raw_records:
            logger.info(
                "No new data available — CDC hasn't published a newer week yet",
                extra={"run_id": run_id, "row_count_out": 0},
            )
            return []

        transformed = [transform_record(r, run_id) for r in raw_records]

        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(
            "Extraction finished",
            extra={
                "run_id": run_id,
                "stage": "extract",
                "status": "success",
                "row_count_out": len(transformed),
                "duration_ms": duration_ms,
            },
        )
        return transformed

    except Exception as e:
        logger.error(
            "Extraction failed",
            extra={"run_id": run_id, "stage": "extract", "status": "failure", "error_message": str(e)},
        )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    data = run_extraction()
    print(f"Extracted {len(data)} new records")