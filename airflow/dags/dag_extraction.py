import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

AIRFLOW_HOME=os.getenv("AIRFLOW_HOME")

with DAG(
    "Data-Extraction",
    tags=["stream"]
) as dag:
    
    start_producer = BashOperator(
        task_id="kafka_producer",
        cwd=AIRFLOW_HOME,
        bash_command="python ../scripts/message_producer.py"
    )

    start_consumer = BashOperator(
        task_id="kafka_consumer",
        cwd=AIRFLOW_HOME,
        bash_command="python ../scripts/message_consumer.py"
    )

    start_producer >> start_consumer
