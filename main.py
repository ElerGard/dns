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

query21 = '''With to_select As (
                Select products.Индекс As ИндексИД, branches.Индекс as ФилиалИД, products.Наименование as Товар, 
                        branches.Наименование as Филиалы, Sum(Количество) As Количество from sales
                JOIN products ON products.Ссылка = Номенклатура
                JOIN branches ON branches.Ссылка = Филиал 
                GROUP BY products.Индекс, branches.Индекс, Товар, Филиалы
                ORDER BY Количество DESC
            )
            Select ФилиалИД, Филиалы, Sum(Количество) as КоличествоПродаж from to_select 
            Where (position('склад' in lower(Филиалы)) = 0 and position('cклад' in lower(Филиалы)) = 0)
            GROUP BY ФилиалИД, Филиалы
            ORDER BY КоличествоПродаж DESC
            Limit 10
        '''
query22 = '''With to_select As (
                Select products.Индекс As ИндексИД, branches.Индекс as ФилиалИД, products.Наименование as Товар, 
                        branches.Наименование as Филиалы, Sum(Количество) As Количество from sales
                JOIN products ON products.Ссылка = Номенклатура
                JOIN branches ON branches.Ссылка = Филиал 
                GROUP BY products.Индекс, branches.Индекс, Товар, Филиалы
                ORDER BY Количество DESC
            )
            Select ФилиалИД, Филиалы, Sum(Количество) as КоличествоПродаж from to_select 
            Where (position('склад' in lower(Филиалы)) > 0 or position('cклад' in lower(Филиалы)) > 0)
            GROUP BY ФилиалИД, Филиалы
            ORDER BY КоличествоПродаж DESC
            Limit 10
        '''
query23 = '''With to_select As (
                Select products.Индекс As ТоварИД, branches.Индекс as ФилиалИД, products.Наименование as Товар, 
                        branches.Наименование as Филиалы, Sum(Количество) As Количество from sales
                JOIN products ON products.Ссылка = Номенклатура
                JOIN branches ON branches.Ссылка = Филиал 
                GROUP BY ТоварИД, ФилиалИД, Товар, Филиалы
                ORDER BY Количество DESC
            )
            Select ФилиалИД, ТоварИД, Филиалы, Товар, Sum(Количество) as КоличествоПродаж from to_select 
            Where (position('склад' in lower(Филиалы)) > 0 or position('cклад' in lower(Филиалы)) > 0)
            GROUP BY ФилиалИД, ТоварИД, Филиалы, Товар
            ORDER BY КоличествоПродаж DESC
            Limit 10
        '''
query24 = '''With to_select As (
                Select products.Индекс As ТоварИД, branches.Индекс as ФилиалИД, products.Наименование as Товар, 
                        branches.Наименование as Филиалы, Sum(Количество) As Количество from sales
                JOIN products ON products.Ссылка = Номенклатура
                JOIN branches ON branches.Ссылка = Филиал 
                GROUP BY ТоварИД, ФилиалИД, Товар, Филиалы
                ORDER BY Количество DESC
            )
            Select ФилиалИД, ТоварИД, Филиалы, Товар, Sum(Количество) as КоличествоПродаж from to_select 
            Where (position('склад' in lower(Филиалы)) = 0 and position('cклад' in lower(Филиалы)) = 0)
            GROUP BY ФилиалИД, ТоварИД, Филиалы, Товар
            ORDER BY КоличествоПродаж DESC
            Limit 10
        '''
cursor = conn.cursor()
for query in queries:
    cursor.execute(query)
cursor.close()
conn.commit()

conn_string = 'postgresql://postgres:1111@localhost/dns'
  
db = create_engine(conn_string)
conn2 = db.connect()

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
query1 = '''With storages_sales As (
                Select Ссылка, Наименование From branches Where (position('склад' in lower(КраткоеНаименование)) > 0)
            )
            Select Наименование, SUM(Количество) As Количество_продаж From storages_sales
            JOIN sales ON Ссылка = Филиал
            GROUP BY Наименование
            ORDER BY Количество_продаж DESC
            LIMIT 10
        '''
st = time.time()
query2 = '''With storages_sales As (
                Select Ссылка, Наименование From branches Where (position('склад' in lower(КраткоеНаименование)) = 0)
            )
            Select Наименование, SUM(Количество) As Количество_продаж From storages_sales
            JOIN sales ON Ссылка = Филиал
            GROUP BY Наименование
            ORDER BY Количество_продаж DESC
            LIMIT 10
        '''
query3 ='''With storages_sales As (
                Select Ссылка, Наименование From branches Where (position('склад' in lower(КраткоеНаименование)) > 0)
            ), test As (
                Select Наименование, Номенклатура, SUM(Количество)  As Количество_продаж From storages_sales
                JOIN sales ON Ссылка = Филиал
                GROUP BY Наименование, Номенклатура
                ORDER BY Количество_продаж DESC
            )
            Select test.Наименование as Магазин, products.Наименование as Товар, 
                Количество_продаж from test
            JOIN products ON Ссылка = Номенклатура
        '''
# Если необходимо исключить из товара Доставку грузов, обработку грузов, доставку внутри региона, то просто добавить условие
# Where (position('грузов' in lower(Наименование)) = 0 and position('доставка' in lower(Наименование)) = 0)
# К Select * form to_select
query4 = '''With products_without_delivery As (
                Select * From products Where (position('грузов' in lower(Наименование)) = 0 and position('доставка' in lower(Наименование)) = 0)
            )
                Select products_without_delivery.Индекс, products_without_delivery.Наименование as Товар, branches.Наименование as Филиалы, Sum(Количество) As Количество from sales
                JOIN products_without_delivery ON products_without_delivery.Ссылка = Номенклатура
                JOIN branches ON branches.Ссылка = Филиал 
                GROUP BY products_without_delivery.Индекс, Товар, Филиалы
                ORDER BY Количество DESC
            )
            Select * from to_select 
        '''
# df = pd.read_sql_query(query1, conn2)
# print(df)
df = pd.read_sql_query(query3, conn2)
print(df)
et = time.time()
print(et - st)
# cursor = conn.cursor()
# cursor.execute(query)
# row = cursor.fetchall()
# print(row)
# cursor.close()

# Execution time SQL: 0.9208459854125977 seconds
# Execution time SQL: 0.9264271259307861 seconds
# Execution time SQL: 0.9046401977539062 seconds
# Execution time SQL: 0.9212141036987305 seconds
# Execution time Pandas DF: 0.951648473739624 seconds
# Execution time Pandas DF: 0.9107892513275146 seconds
# Execution time Pandas DF: 0.9167971611022949 seconds
# Execution time Pandas DF: 0.9107892513275146 seconds

def first_task():
    pass

def second_task():
    pass

def third_task():
    pass

conn.close()
