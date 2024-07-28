from airflow import DAG
from airflow.operators.bash import BashOperator
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

    
    ingest_tasks = []
    for data_asset in data_assets:
        task = BashOperator(
            task_id=f'ingest_data_{data_asset}',
            bash_command=f"python3 /mnt/etl/ingestion/request_ingestion.py --data_asset {data_asset}"
        )
        ingest_tasks.append(task)
    
    clean_tasks = []
    for data_asset in data_assets:
        file_path = f"{{{{ task_instance.xcom_pull(task_ids='ingest_data_{data_asset}') }}}}"
        task = BashOperator(
            task_id=f'clean_data_{data_asset}',
            bash_command=f'''spark-submit \
                --master spark://master-spark:7077 \
                --name clean_data_{data_asset} \
                /mnt/etl/cleansing/clean_test.py --data_asset {data_asset} --file_path {file_path}''',
        )
        clean_tasks.append(task)
    
    
    for i in range(len(data_assets)):
        ingest_tasks[i] >> clean_tasks[i]
    