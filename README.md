# Stream Data Pipeline

Showcase for ELT data pipelines with streaming data via Kafka & Spark as well as
DuckDB, dbt, Apache Airflow and Superset.

The data is provided by [Wikimedia](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service).

## Prerequisites

```sh
pip install -r requirements.txt
```

## Commands

Start Kafka Streaming: `docker compose up`

Start Data Extraction: `./extract_data.sh`

Load Data: `python scripts/load_data.py`

Transform Data: `dbt run --project-dir transform/wikimedia --profiles-dir transform/wikimedia --profile wikimedia`

Data Tests: `dbt test --project-dir transform/wikimedia --profiles-dir transform/wikimedia --profile wikimedia`

