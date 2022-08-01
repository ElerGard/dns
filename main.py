import numpy as np
import pandas as pd
import plotly
import psycopg2
import time
from pyparsing import col
from sqlalchemy import create_engine

try:
    conn = psycopg2.connect(
        host='localhost',
        database='dns',
        user='postgres',
        password='1111'
    )
    
except psycopg2.OperationalError:
    print("Не удалось подключиться к базе данных")
    exit()

queries = (
    '''CREATE TABLE IF NOT EXISTS cities (
            Индекс int, 
            Ссылка varchar(60),
            Наименование varchar(60)
        );
    ''',
    '''CREATE TABLE IF NOT EXISTS products (
            Индекс int, 
            Ссылка varchar(60),
            Наименование varchar(60)
        );
    ''',
    '''CREATE TABLE IF NOT EXISTS sales (
            Индекс int, 
            Период timestamp,
            Филиал varchar(60),
            Номенклатура varchar(60),
            Количество real,
            Продажа real
        );
    ''',
    '''CREATE TABLE IF NOT EXISTS branches (
            Индекс int, 
            Ссылка varchar(60),
            Наименование varchar(60),
            Город varchar(60),
            КраткоеНаименование varchar(60),
            Регион varchar(60)
        );
    '''
)

cursor = conn.cursor()
for query in queries:
    cursor.execute(query)
cursor.close()
conn.commit()

# conn_string = 'postgresql://postgres:1111@localhost/dns'
  
# db = create_engine(conn_string)
# conn2 = db.connect()

# files = ['products', 'cities', 'branches', 'sales']
# st = time.time()
# for filename in files:
#     df = pd.read_csv(f't_{filename}.csv', sep=',')
#     df = df.rename(columns={'Unnamed: 0': 'Индекс'})
#     df.to_sql(filename, con=conn2, if_exists='replace',index=False)
# et = time.time()
# ts = st - et

# Не совсем понял как отличать магазины и склады. 
# За склады считал все строки где есть слово склад в Кратком наименовании, остально как магазины
query = '''Select * From branches Where position('склад' in lower(КраткоеНаименование)) > 0'''
cursor = conn.cursor()
cursor.execute(query)
row = cursor.fetchall()
print(row)
cursor.close()

def first_task():
    pass

def second_task():
    pass

def third_task():
    pass

conn.close()
