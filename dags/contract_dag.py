from multiprocessing.spawn import prepare

import pendulum
import sql_scripts
import logging
import etl
from datetime import timedelta
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
import glob
logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 11, 6, tz="Asia/Almaty"),
    catchup=False,
    template_searchpath='',
    tags=["gov-contracts", "version 1.74"]
)

def contracts():
    # A dummy task to start the DAG
    start = BashOperator(task_id="start", bash_command="echo start")

    # A task group to process the data
    with TaskGroup('clear_data') as clear_data:
        # Очистка
        clear_data_task = PythonOperator(
            task_id="clear_data",
            python_callable=etl.Loader.clear_data
        )
        # Удаление таблиц
        drop_data_tables = PostgresOperator(
            task_id="drop_tables",
            postgres_conn_id='local_pgdb',
            sql=sql_scripts.sql_drop_tables
        )

        # Создание таблиц
        create_data_tables = PostgresOperator(
            task_id="create_tables",
            postgres_conn_id='local_pgdb',
            sql=sql_scripts.sql_create_tables
        )
        clear_data_task >> drop_data_tables >> create_data_tables

    with TaskGroup('process_data') as process_data:
        # Загрузка справочника ОКПД с интернет-ресурса
        download_okpd = PythonOperator(
            task_id="download_okpd",
            python_callable=etl.Loader.download_file,
            op_kwargs = {"url": 'https://ofdata.ru/open-data/download/okpd_2.json.zip', "filename": 'okpd_2.json.zip',
                         "downloadpath": 'data/'},
        )

        # Распаковка справочника
        unzip_okpd = PythonOperator(
            task_id="unzip_okpd",
            python_callable=etl.Loader.unzip_file,
            op_kwargs={"filename" : 'data/okpd_2.json.zip', "extract_dir" : 'data/'}
        )

        # Загрузка json файла и сохранения в формате sql для дальнейшей загрузки
        load_json_okpd = PythonOperator(
            task_id="load_json_okpd",
            python_callable=etl.Loader.load_json_okpd,
            op_kwargs={"filename" : 'data/okpd_2.json'}
        )

        # Загрузка данных в хранилище
        load_dims_task = PostgresOperator(
            task_id="load_dims_to_database",
            postgres_conn_id='local_pgdb',
            sql='data/sql/dims.sql'
        )
        download_okpd >> unzip_okpd >> load_json_okpd >> load_dims_task

    with TaskGroup('import_data') as import_data:
        # Удаление таблиц
        drop_data_tables = PostgresOperator(
            task_id="drop_tables",
            postgres_conn_id='local_pgdb',
            sql=sql_scripts.sql_drop_tables_contracts
        )

        # Создание таблиц
        create_data_tables = PostgresOperator(
            task_id="create_tables",
            postgres_conn_id='local_pgdb',
            sql=sql_scripts.sql_create_tables_contracts
        )

        #Разбиение файла на части
        cutting_csv_file = PythonOperator(
            task_id="cutting_csv_file",
            python_callable=etl.Loader.cutting_csv_file,
        )

        # Распределение по группам классификаторов
        update_data_tables = PostgresOperator(
            task_id="update_data_tables",
            postgres_conn_id='local_pgdb',
            sql=sql_scripts.sql_scripts_update
        )

        # Импорт файлов
        # import_csv_file = PythonOperator(
        #     task_id="import_csv_file",
        #     python_callable=etl.Loader.import_contracts,
        #     op_kwargs={"filename": '/opt/airflow/logs/fz.csv'}
        # )
        drop_data_tables >> create_data_tables >> cutting_csv_file >> update_data_tables

    # #Разбиение файла на части
    # cutting_csv_file = PythonOperator(
    #     task_id="cutting_csv_file",
    #     python_callable=etl.Loader.cutting_csv_file,
    # )

    # create_contracts_sql = PythonOperator(
    #     task_id="create_contracts_sql",
    #     python_callable=etl.Loader.create_contracts_sql
    # )

    # # Загрузка фактов в хранилище
    # load_contracts_task = PostgresOperator(
    #     task_id="load_contracts_to_database",
    #     postgres_conn_id='local_pgdb',
    #     sql=glob.glob("data/sql/contracts*.sql")
    # )

    # @task
    # def create_contracts_sql(i):
    #     t1 = PythonOperator(
    #         task_id=f"create_contracts_sql{i}",
    #         python_callable=etl.Loader.create_contracts_sql_file,
    #         op_kwargs={"file_id": {i}}
    #     )
    #
    # task_create_contacts_sql = [create_contracts_sql(i) for i in range(1,12)]

    # @task
    # def save_contracts(i):
    #     t1 = PostgresOperator(
    #         task_id=f'load_contracts_to_database_{i}',
    #         postgres_conn_id='local_pgdb',
    #         sql=f'contracts_{i}.sql'
    #     )
    # tasks_save_contracts = [save_contracts(i) for i in range(1,11)]
    # my_tasks = [load_something.override(task_id=f'load_something_{i+1}')(i) for i in range(1,9)]

    # # Начало
    # start_task = DummyOperator(task_id='start_task', dag=dag)
    #
    # # Удаление таблиц
    # drop_data_tables = PostgresOperator(
    #     task_id = "drop_tables",
    #     postgres_conn_id='local_pgdb',
    #     sql = sql_scripts.sql_drop_tables_contracts
    # )
    #
    # # Создание таблиц
    # create_data_tables = PostgresOperator(
    #     task_id = "create_tables",
    #     postgres_conn_id='local_pgdb',
    #     sql = sql_scripts.sql_create_tables_contracts
    # )
    #
    # #Разбиение файла на части
    # cutting_csv_file = PythonOperator(
    #     task_id="cutting_csv_file",
    #     python_callable=etl.Loader.cutting_csv_file,
    # )
    #
    # # Конец
    # end_task = DummyOperator(task_id='end_task', dag=dag)

    end = BashOperator(task_id="end", bash_command="echo end")

    chain(start, clear_data, process_data, import_data, end)

contracts()