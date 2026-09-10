# Health Data Observability Platform

A data pipeline for CDC public health surveillance data (COVID-19 deaths and flu activity levels), built with an emphasis on **trust, not just movement**. Most portfolio ETL projects stop at "get data from API into warehouse." This one adds the layer that actually makes a pipeline production-worthy: validation that stops bad data before it's trusted, anomaly detection that flags unusual-but-valid data, structured logging that makes every run auditable, and CI/CD that catches regressions before they ship.

![CI](https://github.com/LEAKONO/health-data-observability-platform/actions/workflows/ci.yml/badge.svg)

## Why this project

Every week, the CDC publishes state-level COVID death counts and flu activity levels. Like most real-world government data, it's messy: numbers get revised after publication, some states report late or not at all, and occasional data-entry errors slip through. This project builds a pipeline that doesn't just move that data into a warehouse — it **validates it, remembers its full history, and watches itself for problems**, so that anyone querying the resulting tables can trust what they're looking at.

## Architecture

```mermaid
flowchart TD
    A[CDC Socrata API<br/>COVID deaths + flu ARI<br/>weekly]
    A --> B[Extract<br/>incremental]
    B --> C[Load<br/>staged bulk + MERGE]
    C --> D[(RAW<br/>immutable history)]
    D --> E{Validate<br/>Great Expectations}
    E -->|pass| F[Anomaly detection<br/>z-score per state]
    F --> G[dbt<br/>staging to marts]
    G --> H[Observability views<br/>pure SQL]

    subgraph Orchestration
        I[Airflow<br/>weekly + retries]
    end
    subgraph "Quality gate"
        J[GitHub Actions<br/>lint, test, dbt build]
    end

    I -.triggers.-> B
    J -.validates.-> G

    classDef source fill:#E6F1FB,stroke:#185FA5,stroke-width:1.5px,color:#0C447C
    classDef process fill:#EEEDFE,stroke:#534AB7,stroke-width:1.5px,color:#3C3489
    classDef storage fill:#F1EFE8,stroke:#5F5E5A,stroke-width:1.5px,color:#2C2C2A
    classDef gate fill:#FAEEDA,stroke:#854F0B,stroke-width:1.5px,color:#412402
    classDef observe fill:#E1F5EE,stroke:#0F6E56,stroke-width:1.5px,color:#04342C
    classDef infra fill:#FBEAF0,stroke:#993556,stroke-width:1.5px,color:#4B1528

    class A source
    class B,C,F,G process
    class D storage
    class E gate
    class H observe
    class I,J infra
```

Every stage logs to `OBSERVABILITY.PIPELINE_LOGS`. Every run's outcome is queryable with plain SQL — no BI tool required.

## Key design decisions

**Immutable raw layer, not overwrite-in-place.** `RAW.COVID_DEATHS_RAW` is keyed on `(run_id, snapshot_date, state)`, not just `(snapshot_date, state)`. When CDC revises a previously-published week, the old version isn't lost — a new row is added. This is what makes `fct_cases_as_reported` (what we knew as of any point in time) and `fct_cases_latest` (current best-known values) both possible from the same source of truth.

**Idempotent loads via `MERGE`, not `INSERT`.** The loader matches on the same composite key as the table's primary key. Re-running the same batch twice never duplicates data — verified directly against live data.

**Incremental extraction, not full re-pull.** Before calling the CDC API, the extractor checks Snowflake for the most recent `snapshot_date` already loaded, and only requests weeks newer than that. Verified live: a full historical backfill (16,200 rows, 6 years of data) followed by a same-day re-run correctly extracted zero new rows and made zero unnecessary database calls.

**Validation (Great Expectations) is separate from anomaly detection, on purpose.** GE catches *invalid* data — negative counts, broken schema, duplicate keys — and is fail-closed: a failed suite should stop the pipeline. The anomaly detector catches *valid but unusual* data — a real spike in a state's death count — and only flags it for review, never fails the pipeline. Conflating these two would either make the pipeline too fragile (failing on real pandemic waves) or too silent (never catching genuine errors).

## Findings — what the observability layer actually caught

- **35% of early COVID records have no `covid_19_deaths` value.** Great Expectations' `total_deaths >= covid_19_deaths` check initially failed on the full historical load — not a bug, but a real reporting gap. Fixed by distinguishing "not yet reported" from "inconsistent."
- **514 real anomalies detected** across 6 years of COVID death data, aligning with known pandemic history — e.g. California peaking at z=16.6 during the winter 2020–21 surge.
- **`pipeline_run_summary` honestly reflects real debugging history** — the COVID load stage shows 3 successes and 2 failures out of 5 runs, matching real bugs found and fixed during development.

## Tech stack

| Layer | Tool | Purpose |
|---|---|---|
| Extraction | Python, `requests` | Pull CDC Socrata API data, incrementally |
| Warehouse | Snowflake | Immutable raw layer, staging, marts, observability schemas |
| Validation | Great Expectations | Fail-closed data quality gates |
| Transformation | dbt | Staging → marts, including as-reported/latest/revision-history modeling |
| Orchestration | Apache Airflow | Weekly scheduling, retries, failure alerting |
| Observability | Custom (Python + Snowflake) | Structured JSON logging, anomaly detection, pipeline health views |
| Alerting | Slack (incoming webhook) | Real-time failure notification |
| CI/CD | GitHub Actions | Lint, unit tests, GE suite validation, DAG import check, dbt build/test |
| Testing | pytest, dbt tests | Code-logic tests and data-quality tests, layered separately |

## Project structure

```
health-data-observability-platform/
├── dags/                          # Airflow DAG
├── dbt_project/                   # staging → marts → observability models
├── great_expectations/            # Validation suites
├── src/
│   ├── extract/                   # CDC API extractors (COVID, flu)
│   ├── load/                      # Snowflake loaders (idempotent MERGE)
│   ├── observability/             # Logging, anomaly detection, alerting
│   ├── validation/                # GE validation runners
│   └── utils/                     # Shared Snowflake connection
├── tests/                         # pytest unit tests
├── .github/workflows/ci.yml       # CI pipeline
└── setup.sql                      # Snowflake schema (source of truth)
```

## Setup

1. **Snowflake**: create a free trial account, then run `setup.sql` in a Snowsight worksheet to create the database, schemas, and tables.
2. **Environment**: copy `.env.example` to `.env` and fill in your Snowflake credentials and (optionally) a Slack webhook URL.
3. **Python**:
```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
```
4. **Run a manual pipeline pass**:
```bash
   python3 -c "
   from dotenv import load_dotenv
   load_dotenv()
   from src.extract.cdc_covid_deaths_extractor import run_extraction
   from src.load.snowflake_loader import load_covid_deaths
   records = run_extraction()
   if records:
       print(load_covid_deaths(records, run_id=records[0]['run_id']))
   "
```
5. **dbt**:
```bash
   cd dbt_project
   cp profiles.yml.example ~/.dbt/profiles.yml
   dbt deps
   dbt build
```
6. **Tests**: `pytest tests/ -v`



## About

Built by **Emmanuel Leakono** as a hands-on exploration of what separates a working pipeline from a trustworthy one — every design decision, bug, and fix in this repo was found and solved against real, live infrastructure, not simulated for the sake of a demo.

- GitHub: [github.com/LEAKONO](https://github.com/LEAKONO)
- Feedback and PRs welcome — if you spot something that could be more robust, open an issue.

If this project was useful or interesting, a ⭐ on the repo is appreciated.