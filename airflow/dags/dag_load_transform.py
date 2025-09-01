import os 
from airflow import DAG
from airflow.provider.standard.operators.bash import BashOperator
from datetime import timedelta

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    "Data-Load_Transformation",
    schedule=timedelta(seconds=90)
) as dag:
    
    load_data = BashOperator(
        task_id="load_data",
        cwd=AIRFLOW_HOME,
        bash_command="python ../scripts/load_data.py"
    )

    transform_data = BashOperator(
        task_id="run_dbt",
        cwd=AIRFLOW_HOME,
        bash_command="dbt run --project-dir ../transform/wikimedia --profiles-dir ../transform/wikimedia --profile wikimedia"
    )

    test_data = BashOperator(
        task_id="test_dbt",
        cwd=AIRFLOW_HOME,
        bash_command="dbt test --project-dir ../transform/wikimedia --profiles-dir ../transform/wikimedia --profile wikimedia"
    )

    load_data >> transform_data >> test_data
