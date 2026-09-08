"""
src/validation/validate_flu_surveillance.py

Runs the flu_surveillance_suite Great Expectations checks against
data in RAW.FLU_SURVEILLANCE_RAW. Same fail-closed pattern as
validate_covid_deaths.py, adapted for this table's categorical schema.
"""

import json
import sys

import pandas as pd
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

from src.utils.snowflake_connection import get_snowflake_connection
from src.observability.logger_config import get_logger

logger = get_logger(__name__)

SUITE_PATH = "great_expectations/expectations/flu_surveillance_suite.json"


def load_data_for_validation(run_id: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    try:
        query = f"""
            SELECT RUN_ID, SNAPSHOT_DATE, STATE, ACTIVITY_LEVEL
            FROM RAW.FLU_SURVEILLANCE_RAW
            WHERE RUN_ID = '{run_id}'
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def run_validation(run_id: str) -> dict:
    df = load_data_for_validation(run_id)

    if df.empty:
        logger.info(
            "No rows to validate for this run",
            extra={"run_id": run_id, "stage": "validate"},
        )
        return {"success": True, "row_count": 0, "failed_expectations": 0, "failure_details": []}

    with open(SUITE_PATH) as f:
        suite_dict = json.load(f)

    project_config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
    context = EphemeralDataContext(project_config=project_config)

    validator = context.sources.pandas_default.read_dataframe(df)

    results = []
    for expectation in suite_dict["expectations"]:
        method = getattr(validator, expectation["expectation_type"])
        result = method(**expectation["kwargs"])
        results.append((expectation["expectation_type"], result))

    all_passed = all(r.success for _, r in results)
    failed = [(name, r) for name, r in results if not r.success]

    if all_passed:
        logger.info(
            "Validation passed",
            extra={"run_id": run_id, "stage": "validate", "status": "success", "row_count_in": len(df)},
        )
    else:
        for name, r in failed:
            logger.error(
                "Expectation failed",
                extra={
                    "run_id": run_id,
                    "stage": "validate",
                    "status": "failure",
                    "expectation": name,
                    "unexpected_count": r.result.get("unexpected_count"),
                    "unexpected_percent": r.result.get("unexpected_percent"),
                    "partial_unexpected_list": r.result.get("partial_unexpected_list"),
                },
            )

    return {
        "success": all_passed,
        "row_count": len(df),
        "failed_expectations": len(failed),
        "failure_details": [{"expectation": name, "result": r.result} for name, r in failed],
    }


if __name__ == "__main__":
    run_id = sys.argv[1]
    result = run_validation(run_id)
    print(f"Success: {result['success']}")
    print(f"Row count: {result['row_count']}")
    if result["failed_expectations"] > 0:
        print("\nFailure details:")
        for detail in result["failure_details"]:
            print(f"  - {detail['expectation']}")
            print(f"    {detail['result']}")
    if not result["success"]:
        sys.exit(1)