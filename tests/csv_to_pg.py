# import libraries
import pandas as pd
from sqlalchemy import create_engine
import time
import csv
from io import StringIO

def psql_insert_copy(table, conn, keys, data_iter): #mehod
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

columns = ["id", "reestrNumber", "iczNumber", "inn", "supplier", "code1",
           "code2", "customer", "customer_area", "code3", "status_contract",
           "description", "budget", "contract_date", "contract_execution_date",
           "contract_end_date", "date1", "date2", "code4", "code5",
           "amount", "contract_price", "code6", "code7",
           "code8", "okpd_name", "okpd_code"]
filename = "C:\\Work\\temp\\cutted\\fz_1.csv"
df = pd.read_csv(filename, header=None, encoding='utf-8', on_bad_lines='skip',
                 low_memory=False, index_col=False, dtype='unicode', names=columns)

# Example: 'postgresql://username:password@localhost:5432/your_database'
engine = create_engine('postgresql://gov_owner:LxarmMyX9AvCJjb4V65N2d@dockerhub.corp.darrail.com:54320/gov_contract')

start_time = time.time() # get start time before insert

df.to_sql(
    name="contracts",
    con=engine,
    if_exists="append",
    index=False,
    method=psql_insert_copy
)

end_time = time.time() # get end time after insert
total_time = end_time - start_time # calculate the time
print(f"Insert time: {total_time} seconds") # print time