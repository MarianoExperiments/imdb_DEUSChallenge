from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DATA_ASSETS = [
    "name.basics",
    "title.basics",
    "title.principals",
    "title.ratings",]

#Define default arguments
DEFAULT_ARGS = {
 'owner': 'DEUS_NEXT_GOAT',
 'start_date': datetime (2024, 7, 25),
 'depends_on_past': True 
}

#Function to modify the paths
def modify_paths(ti):
    modified_paths = {}
    for data_asset in DATA_ASSETS:
        path = ti.xcom_pull(task_ids=f'ingest_data_{data_asset}')
        if path:
            modified_path = path.replace("raw", "clean")
            modified_paths[data_asset] = modified_path
    ti.xcom_push(key='modified_paths', value=modified_paths)
    

def refresh_materialized_view():
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            host="postgres",
            database="challenge",
            user="admin",
            password="admin"
        )

        # Create a cursor object
        cur = conn.cursor()

        # Execute the refresh statement
        cur.execute("REFRESH MATERIALIZED VIEW imdb.actor_movie_details;")

        # Commit the changes
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

        print("Materialized view refreshed successfully")

    except Exception as e:
        print(f"Error refreshing materialized view: {e}")
        raise AirflowException(f"Error refreshing materialized view: {e}")

# Instantiate your DAG
with DAG ('IMDB_ETL', 
          default_args=DEFAULT_ARGS, 
          schedule_interval='@daily',
          start_date=datetime(2023, 7, 28),
          catchup=False, 
          max_active_runs=1,
          description="IMDB: Ingestion and Cleansing Pipeline") as dag:

    
    ingest_tasks = []
    for data_asset in DATA_ASSETS:
        task = BashOperator(
            task_id=f'ingest_data_{data_asset}',
            bash_command=f"python3 /mnt/etl/ingestion/request_ingestion.py --data_asset {data_asset}",
        )
        ingest_tasks.append(task)
        
    paths = {}
    for data_asset in DATA_ASSETS:
        paths[data_asset] = f"{{{{ task_instance.xcom_pull(task_ids='ingest_data_{data_asset}') }}}}"
        
    clean_tasks = []
    for data_asset in DATA_ASSETS:
        task = BashOperator(
            task_id=f'clean_data_{data_asset}',
            bash_command=f'''spark-submit \
                --master spark://master-spark:7077 \
                --driver-class-path /opt/airflow/postgresql-42.7.3.jar --jars postgresql-42.7.3.jar \
                --name clean_data_{data_asset} \
                /mnt/etl/cleansing/clean_task.py --data_asset {data_asset} --file_path {paths[data_asset]} --operation clean''',
        )
        clean_tasks.append(task)
    
    modify_paths_task = PythonOperator(
        task_id='modify_paths',
        python_callable=modify_paths,
    )
    
    paths_mod = {}
    for data_asset in DATA_ASSETS:
        paths_mod[data_asset] = f"{{{{ task_instance.xcom_pull(task_ids='modify_paths', key='modified_paths')['{data_asset}'] }}}}"
    
    load_tasks = []
    for data_asset in DATA_ASSETS:
        # paths[data_asset] = paths[data_asset].replace("raw", "clean")
        task = BashOperator(
            task_id=f'load_data_{data_asset}',
            bash_command=f'''spark-submit \
                --master spark://master-spark:7077 \
                --driver-class-path /opt/airflow/postgresql-42.7.3.jar --jars postgresql-42.7.3.jar \
                --name load_data_{data_asset} \
                /mnt/etl/cleansing/clean_task.py --data_asset {data_asset} --file_path {paths_mod[data_asset]} --operation load''',
        )
        load_tasks.append(task)
        
    refresh_mv_task = PythonOperator(
        task_id='refresh_materialized_view',
        python_callable=refresh_materialized_view
    ) 
    
    for i in range(len(DATA_ASSETS)):
        ingest_tasks[i] >> clean_tasks[i] >> modify_paths_task >> load_tasks[i] >> refresh_mv_task
    