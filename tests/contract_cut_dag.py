from multiprocessing.spawn import prepare

import pendulum
import sql_scripts
import logging
import etl
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import glob
logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 11, 7, tz="Asia/Almaty"),
    catchup=False,
    template_searchpath='',
    tags=["gov-contracts-cut", "version 1.2"]
)

def contracts_cut():
    # Начало
    start_task = DummyOperator(task_id='start_task', dag=dag)

    # Удаление таблиц
    drop_data_tables = PostgresOperator(
        task_id = "drop_tables",
        postgres_conn_id='local_pgdb',
        sql = sql_scripts.sql_drop_tables_contracts
    )

    # Создание таблиц
    create_data_tables = PostgresOperator(
        task_id = "create_tables",
        postgres_conn_id='local_pgdb',
        sql = sql_scripts.sql_create_tables_contracts
    )

    #Разбиение файла на части
    cutting_csv_file = PythonOperator(
        task_id="cutting_csv_file",
        python_callable=etl.Loader.cutting_csv_file,
    )

    # Конец
    end_task = DummyOperator(task_id='end_task', dag=dag)

    start_task >> drop_data_tables >> create_data_tables >> cutting_csv_file >> end_task

contracts_cut()