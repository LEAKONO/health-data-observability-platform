import json
import time
from datetime import datetime, timezone

from src.utils.snowflake_connection import get_snowflake_connection
from src.observability.logger_config import get_logger

logger = get_logger(__name__)


def load_covid_deaths(records: list[dict], run_id: str) -> dict:
    if not records:
        logger.info(
            "No records to load", extra={"run_id": run_id, "stage": "load", "row_count_in": 0}
        )
        return {"row_count_in": 0, "row_count_out": 0, "status": "success"}

    start_time = time.time()
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    row_count_out = 0
    status = "success"
    error_message = None

    try:
        cursor.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS RAW.COVID_DEATHS_STAGING (
                run_id            STRING,
                snapshot_date     DATE,
                ingested_at       TIMESTAMP_NTZ,
                state             STRING,
                covid_19_deaths   INTEGER,
                total_deaths      INTEGER,
                pneumonia_deaths  INTEGER,
                pneumonia_and_covid_19_deaths INTEGER,
                influenza_deaths  INTEGER,
                raw_payload_text  STRING
            )
        """)

        insert_rows = [
            (
                r["run_id"], r["snapshot_date"], r["ingested_at"], r["state"],
                r["covid_19_deaths"], r["total_deaths"], r["pneumonia_deaths"],
                r["pneumonia_and_covid_19_deaths"], r["influenza_deaths"],
                json.dumps(r["raw_payload"]),
            )
            for r in records
        ]
        cursor.executemany(
            """
            INSERT INTO RAW.COVID_DEATHS_STAGING
            (run_id, snapshot_date, ingested_at, state, covid_19_deaths,
             total_deaths, pneumonia_deaths, pneumonia_and_covid_19_deaths,
             influenza_deaths, raw_payload_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            insert_rows,
        )

        cursor.execute("""
            MERGE INTO RAW.COVID_DEATHS_RAW AS target
            USING (
                SELECT
                    run_id, snapshot_date, ingested_at, state,
                    covid_19_deaths, total_deaths, pneumonia_deaths,
                    pneumonia_and_covid_19_deaths, influenza_deaths,
                    PARSE_JSON(raw_payload_text) AS raw_payload
                FROM RAW.COVID_DEATHS_STAGING
            ) AS source
            ON target.run_id = source.run_id
               AND target.snapshot_date = source.snapshot_date
               AND target.state = source.state
            WHEN MATCHED THEN UPDATE SET
                ingested_at = source.ingested_at,
                covid_19_deaths = source.covid_19_deaths,
                total_deaths = source.total_deaths,
                pneumonia_deaths = source.pneumonia_deaths,
                pneumonia_and_covid_19_deaths = source.pneumonia_and_covid_19_deaths,
                influenza_deaths = source.influenza_deaths,
                raw_payload = source.raw_payload
            WHEN NOT MATCHED THEN INSERT (
                run_id, snapshot_date, ingested_at, state, covid_19_deaths,
                total_deaths, pneumonia_deaths, pneumonia_and_covid_19_deaths,
                influenza_deaths, raw_payload
            ) VALUES (
                source.run_id, source.snapshot_date, source.ingested_at, source.state,
                source.covid_19_deaths, source.total_deaths, source.pneumonia_deaths,
                source.pneumonia_and_covid_19_deaths, source.influenza_deaths, source.raw_payload
            )
        """)
        row_count_out = len(records)

        cursor.execute("DROP TABLE IF EXISTS RAW.COVID_DEATHS_STAGING")

    except Exception as e:
        status = "failure"
        error_message = str(e)
        logger.error(
            "Load failed",
            extra={"run_id": run_id, "stage": "load", "status": status, "error_message": error_message},
        )
        raise

    finally:
        duration_ms = int((time.time() - start_time) * 1000)
        _write_pipeline_log(
            conn=conn,
            run_id=run_id,
            pipeline_name="covid_deaths_pipeline",
            stage="load",
            status=status,
            row_count_in=len(records),
            row_count_out=row_count_out,
            duration_ms=duration_ms,
            error_message=error_message,
        )
        cursor.close()
        conn.close()

    logger.info(
        "Load finished",
        extra={
            "run_id": run_id,
            "stage": "load",
            "status": status,
            "row_count_in": len(records),
            "row_count_out": row_count_out,
            "duration_ms": duration_ms,
        },
    )
    return {"row_count_in": len(records), "row_count_out": row_count_out, "status": status}


def _write_pipeline_log(conn, run_id, pipeline_name, stage, status,
                         row_count_in, row_count_out, duration_ms, error_message):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO OBSERVABILITY.PIPELINE_LOGS
                (run_id, pipeline_name, stage, status, row_count_in,
                 row_count_out, duration_ms, error_message)
            VALUES (%(run_id)s, %(pipeline_name)s, %(stage)s, %(status)s,
                    %(row_count_in)s, %(row_count_out)s, %(duration_ms)s, %(error_message)s)
            """,
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "stage": stage,
                "status": status,
                "row_count_in": row_count_in,
                "row_count_out": row_count_out,
                "duration_ms": duration_ms,
                "error_message": error_message,
            },
        )
    finally:
        cursor.close()