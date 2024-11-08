import pandas as pd
from datetime import datetime

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

def is_string(string):
    return False if isinstance(string, str) else False

def is_number(number):
    return True if isinstance(number, int) or isinstance(number, (float, int)) else False

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

pd.set_option("display.max_rows", None)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

filename = "C:/Work/temp/fzsmall.csv"
# chunksize = 10 ** 6
# for chunk in pd.read_csv(filename, chunksize=chunksize):
#     # chunk is a DataFrame. To "process" the rows in the chunk:
#     for index, row in chunk.iterrows():
#         print(row)
columns = ["id", "reestrNumber", "iczNumber", "inn", "supplier", "code1",
                                               "code2", "customer", "customer_area", "code3", "status_contract",
                                               "description", "budget", "contract_date", "contract_execution_date",
                                               "contract_end_date", "date1", "date2", "code4", "code5",
                                               "amount", "contract_price", "code6", "code7",
                                               "code8", "okpd_name", "okpd_code"]
#df = pd.read_csv(filename, index_col=0, names=columns)
df = pd.read_csv(filename, header=None, encoding='utf-8', on_bad_lines='skip',
                 low_memory=False, index_col=False, names=columns)
format = "%Y-%m-%d"
df = df.drop(columns=['code1', 'code2', 'code3', 'date1', 'date2', 'code4', 'code5', 'code6', 'code7', 'code8'])
df = df.replace("\\N", '')


df['contract_price'] = df['contract_price'].apply(lambda x: convert_to_number(x))
df['contract_price'] = df['contract_price'].fillna(0)
df['contract_price']  = df['contract_price'].replace("", 0)

df['amount'] = df['amount'].apply(lambda x: convert_to_number(x))
df['amount'] = df['amount'].fillna(0)
df['amount'] = df['amount'].replace("", 0)

df['contract_date'] = df['contract_date'].apply(lambda x: x if check_date(x) else None)

#df['contract_price'] = df['contract_price'].apply(lambda x: x if not check_number(x) else 0)
#df['contract_price'] = df['contract_price'].apply(lambda x: x if is_string(x) else 0)
#df['amount'] = df['amount'].apply(lambda x: x if is_string(x) else 0)
#df['amount'] = df['amount'].apply(lambda x: x if not check_number(x) else 0)



df = df.fillna('')

# print(df.info())
#df = dd.read_csv(filename, header=None, names=columns, blocksize=25e6)
#print(df.info())
#print(df.describe())
print(df.head(15))
print(df.tail(15))
#print(df.isnull().sum())
# df2 = pd.DataFrame(df, columns=columns)
# for column in df.columns:
#     print(column)
# print(df2.info())
# print(df2.describe())
