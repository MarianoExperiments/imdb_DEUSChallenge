from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from test import test
from datetime import datetime

#Define default arguments
default_args = {
 'owner': 'DEUS_NEXT_GOAT',
 'start_date': datetime (2024, 7, 25),
}

# Instantiate your DAG
with DAG ('IMDB_ETL', default_args=default_args, schedule_interval=None, description="IMDB: Ingestion and Cleansing Pipeline") as dag:

    ingestion = PythonOperator(
        task_id='IMDB_Ingestion',
        python_callable=test,
    )

    # Set task dependencies
    ingestion