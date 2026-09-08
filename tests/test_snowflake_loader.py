
from unittest.mock import MagicMock, patch

from src.load.snowflake_loader import load_covid_deaths


def make_sample_record(run_id="run_test_001", state="California"):
    return {
        "run_id": run_id,
        "snapshot_date": "2026-09-01",
        "ingested_at": "2026-09-08T06:00:00+00:00",
        "state": state,
        "covid_19_deaths": 15,
        "total_deaths": 900,
        "pneumonia_deaths": 50,
        "pneumonia_and_covid_19_deaths": 10,
        "influenza_deaths": 5,
        "raw_payload": {"state": state, "covid_19_deaths": "15"},
    }


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_succeeds_and_returns_correct_counts(mock_get_conn):
    """A normal load should report row_count_in == row_count_out on success."""
    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_conn

    records = [make_sample_record()]
    result = load_covid_deaths(records, run_id="run_test_001")

    assert result["status"] == "success"
    assert result["row_count_in"] == 1
    assert result["row_count_out"] == 1


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_load_uses_merge_not_plain_insert(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_conn

    records = [make_sample_record()]
    load_covid_deaths(records, run_id="run_test_001")

    # Inspect every SQL statement that was executed
    executed_statements = [call.args[0] for call in mock_conn.cursor().execute.call_args_list]
    merge_statements = [sql for sql in executed_statements if "MERGE INTO" in sql]

    assert len(merge_statements) > 0, "Loader must use MERGE for idempotent upserts, not plain INSERT"


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_merge_key_matches_primary_key(mock_get_conn):

    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_conn

    records = [make_sample_record()]
    load_covid_deaths(records, run_id="run_test_001")

    executed_statements = [call.args[0] for call in mock_conn.cursor().execute.call_args_list]
    merge_sql = next(sql for sql in executed_statements if "MERGE INTO" in sql)

    assert "target.run_id = source.run_id" in merge_sql
    assert "target.snapshot_date = source.snapshot_date" in merge_sql
    assert "target.state = source.state" in merge_sql


@patch("src.load.snowflake_loader.get_snowflake_connection")
def test_empty_batch_does_not_call_database(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_conn

    result = load_covid_deaths([], run_id="run_test_001")

    assert result["row_count_in"] == 0
    assert result["row_count_out"] == 0
    mock_get_conn.assert_not_called()