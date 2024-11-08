import pandas as pd
import time
from io import StringIO
import psycopg2
import csv
from datetime import datetime

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

def sequence_cut(path_file_in, path_dir_out, count_lines=500000):
    '''Функция sequence_cut разрезает файл с расширением .txt на файлы с расширением .txt c заданным количеством строк.
            Параметры: path_file_in : str
                    Абсолютный или относительный путь до файла с расширением .txt, который нужно разрезать.
                  path_dir_out : str
                    Абсолютный или относительный путь до папки, в которую будут помещаться нарезанные файлы.
                  count_lines :  int, default 500000
                    Количество строк, на которые разрезается исходный файл.
       Возвращаемое значение: None
    '''

    def delete_line_feed_end(line):
        if line[-1:] == '\n':
            return line[:-1]
        return line

    path_dir_out += '\\' + path_file_in.split('\\')[-1][:-4]
    stop_iteration = count_lines - 2
    try:
        file_number = 1
        with open(path_file_in) as file_read:
            line = file_read.readline()
            while line:
                new_file_name = path_dir_out + '_' + str(file_number) + '.csv'
                with open(new_file_name, 'w') as file_write:
                    file_write.write(delete_line_feed_end(line))
                    line = file_read.readline()
                    i = 0
                    while line and (i < stop_iteration):
                        file_write.write('\n')
                        file_write.write(line[:-1])
                        line = file_read.readline()
                        i += 1
                    if line:
                        file_write.write('\n')
                        file_write.write(delete_line_feed_end(line))
                file_number += 1
                line = file_read.readline()
    except Exception as err:
        print('Ошибка: ', err)

def batch_cut(path_file_in, path_dir_out, count_lines=500000):
    '''Функция batch_cut разрезает файл с расширением .txt на файлы с расширением .txt c заданным количеством строк.

      Параметры: path_file_in : str
                 Абсолютный или относительный путь до файла с расширением .txt, который нужно разрезать.
                 path_dir_out : str
                    Абсолютный или относительный путь до папки, в которую будут помещаться нарезанные файлы.
                  count_lines :  int, default 500000
                    Количество строк, на которые разрезается исходный файл.
       Возвращаемое значение: None
    '''

    def delete_line_feed_end(line):
        if line[-1:] == '\n':
            return line[:-1]
        return line

    path_dir_out += '\\' + path_file_in.split('\\')[-1][:-4]
    stop_iteration = count_lines - 2
    try:
        file_number = 1
        with open(path_file_in) as file_read:
            line = file_read.readline()
            while line:
                i = 0
                batch = [delete_line_feed_end(line)]
                line = file_read.readline()
                while line and (i < stop_iteration):
                    batch.append(line[:-1])
                    line = file_read.readline()
                    i += 1
                if line:
                    batch.append(delete_line_feed_end(line))
                new_file_name = path_dir_out + '_' + str(file_number) + '.txt'
                with open(new_file_name, 'w') as file_write:
                    file_write.write('\n'.join(batch))
                file_number += 1
                line = file_read.readline()
    except Exception as err:
        print('Ошибка: ', err)

#batch_cut(filename, dirout, count_lines=10)

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
                             low_memory=False, index_col=False, dtype='unicode', names=columns)
            # df = df.drop(
            #     columns=['code1', 'code2', 'code3', 'date1', 'date2', 'code4', 'code5', 'code6', 'code7', 'code8'])
            if (len(df) == 0):
                break
            df = df.replace("\\N", 'NULL')
            df = df.replace("nan", 'NULL')
            df['amount'] = df['amount'].fillna(0)
            df['contract_price'] = df['contract_price'].fillna(0)
            df['contract_price'] = df['contract_price'].apply(lambda x: x if isinstance(x, (int, float)) else 0)
            df['amount'] = df['amount'].apply(lambda x: x if isinstance(x, (int, float)) else 0)
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

            start_time = time.time()  # get start time before insert

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

            end_time = time.time()  # get end time after insert
            total_time = end_time - start_time  # calculate the time
            print(f"Insert time: {total_time} seconds")  # print time

            skiprows += count_lines
            file_number += 1
    except Exception as err:
        print('Ошибка: ', err)

filename = "C:\\Work\\temp\\fz.csv"
dirout = "C:\\Work\\temp\\cutted\\"

pandas_cut(filename, dirout)
