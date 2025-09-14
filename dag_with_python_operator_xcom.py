from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta, timezone

default_args = {
    'owner': 'Matvei',
    'retries': 2,
    'retry_delay': timedelta(seconds=15) 
}


def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hello {first_name} {last_name}, you are {age}")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Matvei")
    ti.xcom_push(key="last_name", value="Spitsyn")


def get_age(ti):
    ti.xcom_push(key='age', value = 22)


with DAG(
    dag_id = 'dag_with_python_operator_v4',
    start_date = datetime(2025, 9, 10),
    schedule = '@daily',
    default_args = default_args
) as dag:

    task1 = PythonOperator(
        task_id="first_task",
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age
    )

    [task2, task3] >> task1

    