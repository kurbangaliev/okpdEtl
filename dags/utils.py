from datetime import datetime
import logging
import threading
import pandas as pd
from io import StringIO
import psycopg2
import csv
from importer import Importer

logger = logging.getLogger(__name__)

def check_date(datestr):
    # initializing format
    format = "%Y-%m-%d"

    # checking if format matches the date
    res = True

    # using try-except to check for truth value
    try:
        res = bool(datetime.strptime(datestr, format))
    except ValueError:
        res = False
    return res

def check_str(strvalue, len):
    res = ""
    # using try-except to check for truth value
    try:
        res = strvalue[0:len]
    except ValueError:
        res = "NULL"
    return res

def is_string(string):
    return True if isinstance(string, str) else False

def is_number(number):
    return True if isinstance(number, int) or isinstance(number, float) else False

def check_number(str_value):
    if is_number(str_value):
        return True
    if is_string(str_value):
        return False
    if isinstance(int, float):
        return True
    if str_value.isdigit():
        return True
    return False

def isNaN(num):
    return num != num

def convert_to_number(any_value):
    if is_number(any_value):
        return any_value
    if isNaN(any_value):
        return 0
    if any_value is None:
        return 0
    if is_string(any_value):
        value = 0
        try:
            value = float(any_value)
        except ValueError:
            value = 0
        return value

def pandas_cut(path_file_in, path_dir_out, count_lines=500000):
    '''Функция pandas_cut разрезает файл с расширением .txt на файлы с расширением .txt c заданным количеством строк.

           Параметры: path_file_in : str
                        Абсолютный или относительный путь до файла с расширением .txt, который нужно разрезать.
                      path_dir_out : str
                        Абсолютный или относительный путь до папки, в которую будут помещаться нарезанные файлы.
                      count_lines :  int, default 500000
                        Количество строк, на которые разрезается исходный файл.
           Возвращаемое значение: None
        '''
    columns = ["id", "reestrNumber", "iczNumber", "inn", "supplier", "code1",
               "code2", "customer", "customer_area", "code3", "status_contract",
               "description", "budget", "contract_date", "contract_execution_date",
               "contract_end_date", "date1", "date2", "code4", "code5",
               "amount", "contract_price", "code6", "code7",
               "code8", "okpd_name", "okpd_code"]
    df = pd.DataFrame([0])
    path_out = path_dir_out + '\\' + path_file_in.split('\\')[-1][:-4]
    file_number = 1
    skiprows = 0
    try:
        while True:
            df = pd.read_csv(path_file_in, header=None, skiprows=skiprows, nrows=count_lines, encoding='utf-8', on_bad_lines='skip',
                             low_memory=False, index_col=False, names=columns)
            # df = df.drop(
            #     columns=['code1', 'code2', 'code3', 'date1', 'date2', 'code4', 'code5', 'code6', 'code7', 'code8'])
            if (len(df) == 0):
                break
            df = df.replace("\\N", 'NULL')
            df = df.replace("nan", 'NULL')

            df['contract_price'] = df['contract_price'].apply(lambda x: convert_to_number(x))
            df['contract_price'] = df['contract_price'].fillna(0)
            df['contract_price'] = df['contract_price'].replace("", 0)

            df['amount'] = df['amount'].apply(lambda x: convert_to_number(x))
            df['amount'] = df['amount'].fillna(0)
            df['amount'] = df['amount'].replace("", 0)

            df['contract_date'] = df['contract_date'].apply(lambda x: x if check_date(x) else '1970-01-01')
            df['contract_execution_date'] = df['contract_execution_date'].apply(lambda x: x if check_date(x) else '1970-01-01')
            df['contract_end_date'] = df['contract_end_date'].apply(lambda x: x if check_date(x) else '1970-01-01')
            # new_file_name = path_out + '_' + str(file_number) + '.csv'
            # df.to_csv(new_file_name, header=None, index=False)
            # Establish a connection to your PostgreSQL database
            conn = psycopg2.connect(
                dbname='gov_contract',
                user='gov_owner',
                password='LxarmMyX9AvCJjb4V65N2d',
                host='dockerhub.corp.darrail.com',
                port='54320'
            )

            start_time = datetime.now()
            logger.info(f"Begin import time: {start_time}")  # print time

            sio = StringIO()
            writer = csv.writer(sio)
            writer.writerows(df.values)
            sio.seek(0)
            with conn.cursor() as c:
                c.copy_expert(
                    sql="""
                    COPY contracts (
                        id, reestrNumber, iczNumber, inn, supplier, code1,
                           code2, customer, customer_area, code3, status_contract,
                           description, budget, contract_date, contract_execution_date,
                           contract_end_date, date1, date2, code4, code5,
                           amount, contract_price, code6, code7,
                           code8, okpd_name, okpd_code
                    ) FROM STDIN WITH CSV""",
                    file=sio
                )
                conn.commit()

            end_time = datetime.now()  # get end time after insert
            total_time = end_time - start_time  # calculate the time
            logger.info(f"End import time: {end_time}")  # print time
            logger.info(f"Insert time: {total_time} seconds")  # print time

            skiprows += count_lines
            file_number += 1
    except Exception as err:
        print('Ошибка: ', err)

def run_import_contracts(filename):
    logger.info(f"Convert file = {filename}")
    thread = threading.Thread(target=Importer.import_contracts_file, args=filename)
    thread.start()

    logging.info("Main: before joining thread %d.")
    thread.join()
    logging.info("Main: thread %d done")

def import_contracts_file(filename):
    logger.info(f"import_contracts_file = {filename}")
    columns = ["id", "reestrNumber", "iczNumber", "inn", "supplier", "customer", "customer_area",
               "status_contract", "description", "budget", "contract_date", "contract_execution_date",
               "contract_end_date", "amount", "contract_price", "okpd_name", "okpd_code"]

    df = pd.read_csv(filename, header=None, encoding='utf-8', on_bad_lines='skip',
                     low_memory=False, index_col=False, dtype='unicode', names=columns)

    df = df.replace("\\N", '')
    df = df.replace("nan", 'NULL')
    df = df.replace("", 'NULL')
    df['amount'] = df['amount'].fillna(0)
    df['contract_price'] = df['contract_price'].fillna(0)
    df['contract_price'] = df['contract_price'].apply(lambda x: x if isinstance(x, (int, float)) else 0)
    df['contract_date'] = df['contract_date'].apply(lambda x: x if check_date(x) else None)
    df = df.fillna('')

    # Establish a connection to your PostgreSQL database
    conn = psycopg2.connect(
        dbname='gov_contract',
        user='gov_owner',
        password='LxarmMyX9AvCJjb4V65N2d',
        host='dockerhub.corp.darrail.com',
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
    logger.info(f"end import file")  # print time