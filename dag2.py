from anyname.level0 import TezkorDAG, CreateTable
from anyname.dag_names import DETAILED_ASSORTMENT_PRODUCTS, RAW_VENDOR_ASSORTMENT_PRODUCTS
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta, datetime

schema = 'detailed'
target_table = 'assortment_products'


def insert_rows(**context):
    from anyname.utils import get_engine
    import pandas as pd

    clickhouse_engine = get_engine('clickhouse')

    created_at_start = context["data_interval_start"]
    created_at_end = context["data_interval_end"]
    q = """
    insert into {target_schema}.{target_table}
    select
         id
        , origin_id
        , name
        , price
        , currency
        , description
        , unit
        , weight
        , options
        , package_code
        , receipt_unit
        , nds
        , code_id
        , barcode
        , old_price
        , volume
        , quantum
        , status
        , created_at
        , updated_at
        , vendor_public_id
        , catalog_id
        , splitByChar(',', replace(replace(barcodes, '{{', ''), '}}', '')) as barcodes
    from raw.vendor_assortment_products
    where updated_at >= toDateTime('{created_at_gt}', 'UTC')
    and updated_at <= toDateTime('{created_at_lt}', 'UTC')
    """.format(created_at_gt=created_at_start.to_datetime_string(),
               created_at_lt=created_at_end.to_datetime_string(),
               target_schema=schema, target_table=target_table)
    print(q)
    pd.read_sql(q, clickhouse_engine)


dag = TezkorDAG(
    dag_id=DETAILED_ASSORTMENT_PRODUCTS, schedule_interval='*/5 * * * *',
    start_date=datetime(2024, 4, 3, 16, 0, 0), params={'team': 'de'}
)
with dag:
    columns_list = [
        {'name': 'id', 'type': 'UUID'},
        {'name': 'origin_id', 'type': 'String'},
        {'name': 'name', 'type': 'String'},
        {'name': 'price', 'type': 'Int64'},
        {'name': 'currency', 'type': 'String'},
        {'name': 'description', 'type': 'String'},
        {'name': 'unit', 'type': 'String', 'not_null': False},
        {'name': 'weight', 'type': 'Int32', 'not_null': False},
        {'name': 'options', 'type': 'String', 'not_null': False},
        {'name': 'package_code', 'type': 'String'},
        {'name': 'receipt_unit', 'type': 'String'},
        {'name': 'nds', 'type': 'Int64', 'not_null': False},
        {'name': 'code_id', 'type': 'Int64', 'not_null': False},
        {'name': 'barcode', 'type': 'String', 'not_null': False},
        {'name': 'old_price', 'type': 'Int64', 'not_null': False},
        {'name': 'volume', 'type': 'Int32', 'not_null': False},
        {'name': 'quantum', 'type': 'Int32', 'not_null': False},
        {'name': 'status', 'type': 'String'},
        {'name': 'created_at', 'type': 'DateTime(\'UTC\')'},
        {'name': 'actual_from', 'type': 'DateTime(\'UTC\')'},
        {'name': 'vendor_public_id', 'type': 'UUID', 'not_null': False},
        {'name': 'catalog_id', 'type': 'UUID', 'not_null': False},
        {'name': 'barcodes', 'type': 'Array(String)', 'not_null': True},
    ]

    # create table
    create_table_instance = CreateTable(
        'clickhouse', None, schema, target_table, columns_list=columns_list,
        create_stmnt_suffix="engine = ReplicatedReplacingMergeTree(actual_from) order by (origin_id, id, actual_from) partition by toDate(actual_from) TTL toDate(actual_from) + INTERVAL 3 month TO VOLUME 'cold_volume'", conn_id='clickhouse'
    )
    create_table_if_not_exists = create_table_instance.generate_task()

    sensor = ExternalTaskSensor(
        task_id=f'sensor_{RAW_VENDOR_ASSORTMENT_PRODUCTS}',
        external_dag_id=RAW_VENDOR_ASSORTMENT_PRODUCTS,
        execution_date_fn=lambda dt: dt + timedelta(minutes=0),
        mode='reschedule', poke_interval=30, timeout=5*60
    )

    insert_rows_task = PythonOperator(
        task_id='update_state', python_callable=insert_rows
    )

    create_table_if_not_exists >> sensor >> insert_rows_task

if __name__ == '__main__':
    import pendulum as pdl

    # dag.test()
    context = {
        'data_interval_start': pdl.datetime(2023, 7, 11, 0, 0, 0, tz='UTC'),
        'data_interval_end': pdl.datetime(2023, 7, 12, 0, 0, 0, tz='UTC')
    }
    insert_rows(**context)
