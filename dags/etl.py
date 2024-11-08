import json
import logging
import pathlib
import shutil
import pandas as pd
import datetime
import threading
import glob
from dateutil.relativedelta import relativedelta
from sql_transform import Transform
import requests
import zipfile
from utils import pandas_cut
import os
from pathlib import Path
import time
from io import StringIO
import psycopg2
import csv

logger = logging.getLogger(__name__)

class Loader:
    loader_begin_date = datetime.datetime(2021, 9, 1)

    @staticmethod
    def clear_data():
        pathlib.Path('data/').mkdir(parents=True, exist_ok=True)
        pathlib.Path('data/contracts/').mkdir(parents=True, exist_ok=True)
        pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)
        shutil.rmtree('data/')

    @staticmethod
    def download_file(url, filename, downloadpath, **kwargs):
        response = requests.get(url)
        pathlib.Path(f'{downloadpath}').mkdir(parents=True, exist_ok=True)
        # if "content-disposition" in response.headers:
        #     content_disposition = response.headers["content-disposition"]
        #     filename = content_disposition.split("filenam}e=")[1]
        # else:
        #     filename = url.split("/")[-1]
        with open(downloadpath + filename, mode="wb") as file:
            file.write(response.content)
        print(f"Downloaded file {filename}")
        logger.info(f"Downloaded file {filename}")

    @staticmethod
    def unzip_file(filename, extract_dir, **kwargs):
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

    @staticmethod
    def load_json_okpd(filename, **kwargs):
        pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)
        with open(filename, encoding='utf-8') as f:
            data = json.load(f)
            if len(data) > 0:
                df = pd.json_normalize(data)
                Transform.df_to_sql("dim_okpd", df)

    @staticmethod
    def cutting_csv_file():
        filename = "/opt/airflow/logs/fz.csv"
        dirout = "/opt/airflow/data/"
        pandas_cut(filename, dirout)

    @staticmethod
    def create_contracts_sql():
        i = 0
        threads = []
        datadir = Path("data/")
        for file in datadir.glob('contracts*.csv'):
            i += 1
            logger.info(f"Convert file = {file.name}")
            thread = threading.Thread(target=Loader.create_contracts_sql_file, args=(file.name, i))
            threads.append(thread)
            thread.start()

        for index, thread in enumerate(threads):
            logging.info("Main    : before joining thread %d.", index)
            thread.join()
            logging.info("Main    : thread %d done", index)


    def create_contracts_sql_file(filename, index):
        columns = ["id", "reestrNumber", "iczNumber", "inn", "supplier", "code1",
                   "code2", "customer", "customer_area", "code3", "status_contract",
                   "description", "budget", "contract_date", "contract_execution_date",
                   "contract_end_date", "date1", "date2", "code4", "code5",
                   "amount", "contract_price", "code6", "code7",
                   "code8", "okpd_name", "okpd_code"]
        logger.info(f"filename={filename}")
        contracts = pd.read_csv(f"data/{filename}", header=None, encoding='utf-8', on_bad_lines='skip',
                         low_memory=False, index_col=False, dtype='unicode', names=columns)
        Transform.df_to_sql('contracts', contracts, f"contracts_{index}.sql")

    @staticmethod
    def import_all_contracts():
        i = 0
        threads = []
        datadir = Path("data/")
        for file in datadir.glob('contracts*.csv'):
            i += 1
            logger.info(f"Convert file = {file.name}")
            thread = threading.Thread(target=Loader.import_contracts, args=(file.name, i))
            threads.append(thread)
            thread.start()

        for index, thread in enumerate(threads):
            logging.info("Main    : before joining thread %d.", index)
            thread.join()
            logging.info("Main    : thread %d done", index)

    @staticmethod
    def check_date(datestr):
        # initializing format
        format = "%d-%m-%Y"

        # checking if format matches the date
        res = True

        # using try-except to check for truth value
        try:
            res = bool(datetime.strptime(datestr, format))
        except ValueError:
            res = False
        return res

    @staticmethod
    def import_contracts(filename, **kwargs):
        columns = ["id", "reestrNumber", "iczNumber", "inn", "supplier", "code1",
                   "code2", "customer", "customer_area", "code3", "status_contract",
                   "description", "budget", "contract_date", "contract_execution_date",
                   "contract_end_date", "date1", "date2", "code4", "code5",
                   "amount", "contract_price", "code6", "code7",
                   "code8", "okpd_name", "okpd_code"]

        df = pd.read_csv(filename, header=None, encoding='utf-8', on_bad_lines='skip',
                         low_memory=False, index_col=False, dtype='unicode', names=columns)

        df = df.drop(columns=['code1', 'code2', 'code3', 'date1', 'date2', 'code4', 'code5', 'code6', 'code7', 'code8'])

        df = df.replace("\\N", '')
        df = df.replace("nan", 'NULL')
        df = df.replace("", 'NULL')
        df['amount'] = df['amount'].fillna(0)
        df['contract_price'] = df['contract_price'].fillna(0)
        df['contract_price'] = df['contract_price'].apply(lambda x: x if isinstance(x, (int, float)) else 0)
        df['contract_date'] = df['contract_date'].apply(lambda x: x if Loader.check_date(x) else None)
        df = df.fillna('')

        # Establish a connection to your PostgreSQL database
        conn = psycopg2.connect(
            dbname='gov_contract',
            user='gov_owner',
            password='LxarmMyX9AvCJjb4V65N2d',
            host='localhost',
            port='54320'
        )

        start_time = time.time()  # get start time before insert

        sio = StringIO()
        writer = csv.writer(sio)
        writer.writerows(df.values)
        sio.seek(0)
        with conn.cursor() as c:
            c.copy_expert(
                sql="""
                COPY contracts (
                    id, reestrnumber, icznumber, inn, supplier, customer, customer_area, status_contract, 
                    description, budget, contract_date, contract_execution_date, contract_end_date,  
                    amount, contract_price, okpd_name, okpd_code
                ) FROM STDIN WITH CSV""",
                file=sio
            )
            conn.commit()

        end_time = time.time()  # get end time after insert
        total_time = end_time - start_time  # calculate the time
        logger.info(f"Insert time: {total_time} seconds")  # print time