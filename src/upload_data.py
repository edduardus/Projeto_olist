import os
import pandas as pd
import sqlalchemy

str_connection = 'sqlite:///{path}'


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

#file_names = os.listdir(DATA_DIR)

file_names = [i for i in os.listdir(DATA_DIR) if i.endswith('.csv')]

connection = sqlalchemy.create_engine(str_connection.format(path = os.path.join(DATA_DIR, 'olist.db')))

for i in file_names:
    df_tmp = pd.read_csv(os.path.join(DATA_DIR, i))
    table_name = 'tb_' + i.replace('olist_',"").replace('_dataset','').strip('.csv')
    df_tmp.to_sql(table_name, connection)