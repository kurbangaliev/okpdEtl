import json
import pandas as pd

from dags.sql_transform import Transform

with open('../data/okpd_2.json', encoding='utf-8') as f:
  data = json.load(f)
  if (len(data)) > 0:
    df = pd.json_normalize(data)
    df.head()
    df.info()
    Transform.df_to_sql("dims_okpd", df)

#    print(df)
  # for item in data:
  #   print(item)