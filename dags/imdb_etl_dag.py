from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from ingestion import request_ingestion
from cleansing import clean_test
from datetime import datetime

data_assets = [
    "name.basics",
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",]

#Define default arguments
default_args = {
 'owner': 'DEUS_NEXT_GOAT',
 'start_date': datetime (2024, 7, 25),
}

# Instantiate your DAG
with DAG ('IMDB_ETL', default_args=default_args, schedule_interval=None, description="IMDB: Ingestion and Cleansing Pipeline") as dag:

    
    # ingest_tasks = []
    # for data_asset in data_assets:
    #     task = PythonOperator(
    #         task_id=f'ingest_data_{data_asset}',
    #         python_callable=request_ingestion.main,
    #         op_kwargs={"data_asset": data_asset},
    #         provide_context=True,
    #     )
    #     ingest_tasks.append(task)
    
    # clean_tasks = []
    # for data_asset in data_assets:
    #     task = PythonOperator(
    #         task_id=f'clean_data_{data_asset}',
    #         python_callable=clean_test.main,
    #         op_kwargs={"data_asset": data_asset, 'file_path': f"{{{{ task_instance.xcom_pull(task_ids='ingest_data_{data_asset}') }}}}"},
    #         provide_context=True,
    #     )
    #     clean_tasks.append(task)
    # for data_asset in data_assets:
    
    
    task = SparkSubmitOperator(
            task_id='clean_data_test',
            application='/mnt/etl/cleansing/clean_test.py',  # Path to your PySpark script
            name='clean_data_test',
            conn_id='spark_default',
            )
        # clean_tasks.append(task)
    
    # for i in range(len(data_assets)):
    #     ingest_tasks[i] >> clean_tasks[i]
    task