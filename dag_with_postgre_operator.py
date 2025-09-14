from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'Matvei',
    'retries': 2,
    'retry_delay': timedelta(seconds=15) 
}

with DAG(
    dag_id='dag_with_postgre_operator_v2',
    start_date=datetime(2023, 7, 26),
    schedule = '0 0 * * *',
    default_args=default_args
) as dag: 
    task1 = SQLExecuteQueryOperator(
        task_id="create_postgres_table",
        conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_run (
                dt date,
                dag_id character varying,
                PRIMARY KEY (dt, dag_id)
            );
        """
    )

    task2 = SQLExecuteQueryOperator(
        task_id = "insert_into_postgres_table",
        conn_id="postgres_localhost",
        sql = """
            insert into dag_run (dt, dag_id) values ('{{ds}}','{{dag.dag_id}}');
        """
    )
    
    task1 >> task2