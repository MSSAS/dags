from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

default_args = {
    'owner': 'Matvei',
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

DATA_DIR = Path("/opt/airflow/dags/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_CSV = DATA_DIR / "titanic.csv"
AGG_CSV = DATA_DIR / "titanic_agg.csv"

def extract_fn():
    df = pd.read_csv("https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv")
    df.to_csv(RAW_CSV, index=False)

def transform_fn():
    df = pd.read_csv(RAW_CSV)
    gr = df.groupby('Pclass', as_index=False)['Survived'].mean()
    gr.to_csv(AGG_CSV, index=False)

def load_fn():
    hook = PostgresHook(postgres_conn_id="postgres_localhost")  # создай connection в UI
    # по желанию обнуляем таблицу перед загрузкой
    hook.run("TRUNCATE TABLE titanic;")
    # копируем CSV с заголовком
    hook.copy_expert(
        sql="""
            COPY titanic (Pclass, Survived)
            FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ',')
        """,
        filename=str(AGG_CSV)
    )

with DAG(
    dag_id='titanic_to_postgres_v2',
    start_date=datetime(2024, 9, 12),
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['git', 'postgres', 'python']
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract_fn
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform_fn
    )

    task_create_table = SQLExecuteQueryOperator(
        task_id="create_postgres_table",
        conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS titanic (
                Pclass int,
                Survived double precision
            );
        """
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load_fn
    )

    task_extract >> [task_transform, task_create_table] >> task_load
