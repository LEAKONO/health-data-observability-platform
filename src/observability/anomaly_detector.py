

import time

import pandas as pd

from src.utils.snowflake_connection import get_snowflake_connection
from src.observability.logger_config import get_logger

logger = get_logger(__name__)

Z_SCORE_THRESHOLD = 3.0  # values beyond 3 standard deviations are flagged
MIN_HISTORY_POINTS = 8   # need at least this many past weeks to judge "normal" for a state


def load_current_run(conn, run_id: str) -> pd.DataFrame:
    """The new data just loaded in this run -- what we're checking."""
    query = f"""
        SELECT snapshot_date, state, covid_19_deaths
        FROM RAW.COVID_DEATHS_RAW
        WHERE run_id = '{run_id}'
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()  # Snowflake returns uppercase column names by default
    return df


def load_history(conn, run_id: str) -> pd.DataFrame:
  
    query = f"""
        SELECT snapshot_date, state, covid_19_deaths
        FROM RAW.COVID_DEATHS_RAW
        WHERE run_id != '{run_id}'
          AND covid_19_deaths IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    return df


def compute_anomalies(current_df: pd.DataFrame, history_df: pd.DataFrame) -> list[dict]:
    
    anomalies = []

    stats_by_state = history_df.groupby("state")["covid_19_deaths"].agg(["mean", "std", "count"])

    for _, row in current_df.iterrows():
        state = row["state"]
        observed_value = row["covid_19_deaths"]

        if pd.isna(observed_value):
            continue  # can't score a missing value

        if state not in stats_by_state.index:
            continue  # no history at all for this state yet

        mean, std, count = stats_by_state.loc[state, ["mean", "std", "count"]]

        if count < MIN_HISTORY_POINTS:
            continue  # not enough history to judge what's "normal" for this state yet

        if std == 0 or pd.isna(std):
            continue  # no variation to measure against -- avoid divide-by-zero

        z_score = (observed_value - mean) / std

        if abs(z_score) > Z_SCORE_THRESHOLD:
            anomalies.append({
                "source": "covid",
                "state": state,
                "metric_name": "covid_19_deaths",
                "observed_value": float(observed_value),
                "expected_range_low": float(mean - Z_SCORE_THRESHOLD * std),
                "expected_range_high": float(mean + Z_SCORE_THRESHOLD * std),
                "z_score": float(z_score),
            })

    return anomalies


def write_anomaly_flags(conn, run_id: str, anomalies: list[dict]):
   
    if not anomalies:
        return

    cursor = conn.cursor()
    try:
        insert_rows = [
            (
                run_id, a["source"], a["state"], a["metric_name"],
                a["observed_value"], a["expected_range_low"],
                a["expected_range_high"], a["z_score"],
            )
            for a in anomalies
        ]
        cursor.executemany(
            """
            INSERT INTO OBSERVABILITY.ANOMALY_FLAGS
                (run_id, source, state, metric_name, observed_value,
                 expected_range_low, expected_range_high, z_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            insert_rows,
        )
    finally:
        cursor.close()


def run_anomaly_detection(run_id: str) -> dict:
  
    start_time = time.time()
    conn = get_snowflake_connection()

    try:
        current_df = load_current_run(conn, run_id)

        if current_df.empty:
            logger.info(
                "No rows to check for anomalies",
                extra={"run_id": run_id, "stage": "anomaly_detection"},
            )
            return {"anomalies_found": 0}

        history_df = load_history(conn, run_id)
        anomalies = compute_anomalies(current_df, history_df)

        if anomalies:
            write_anomaly_flags(conn, run_id, anomalies)

        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(
            "Anomaly detection finished",
            extra={
                "run_id": run_id,
                "stage": "anomaly_detection",
                "status": "success",
                "row_count_in": len(current_df),
                "anomalies_found": len(anomalies),
                "duration_ms": duration_ms,
            },
        )
        return {"anomalies_found": len(anomalies), "anomalies": anomalies}

    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    run_id = sys.argv[1]
    result = run_anomaly_detection(run_id)
    print(f"Anomalies found: {result['anomalies_found']}")
    for a in result.get("anomalies", []):
        print(f"  {a['state']}: observed {a['observed_value']}, expected {a['expected_range_low']:.1f}-{a['expected_range_high']:.1f}, z={a['z_score']:.2f}")