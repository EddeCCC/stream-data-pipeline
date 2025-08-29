# Stream Data Pipeline

Showcase for streaming data pipelines with ...

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

