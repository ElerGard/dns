import numpy as np
import pandas as pd
import plotly.express as px
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
query25 = '''With to_select As (
                Select products.Индекс As ТоварИД, branches.Индекс as ФилиалИД, products.Наименование as Товар, 
                        branches.Наименование as Филиалы, Sum(Количество) As Количество from sales
                JOIN products ON products.Ссылка = Номенклатура
                JOIN branches ON branches.Ссылка = Филиал 
                GROUP BY ТоварИД, ФилиалИД, Товар, Филиалы
                ORDER BY Количество DESC
            ), to_select_city AS (
                Select to_select.*, branches.Город from to_select 
                JOIN branches ON ФилиалИД = Индекс
            ) 
            Select ТоварИД, ФилиалИД as ГородИД, Товар, cities.Наименование, Количество from to_select_city
            JOIN cities ON Город = cities.Ссылка
            LIMIT 10
        '''
task2 = '''With test as (
                SELECT
                EXTRACT (DOW FROM CAST(Период as timestamp)) AS ДеньНедели,
                EXTRACT (HOUR FROM CAST(Период as timestamp)) AS ЧасДня,
                Sum(Количество) as КоличествоПродаж
                FROM
                    sales
                GROUP BY ДеньНедели, ЧасДня
            ), max_sales as (
                Select ДеньНедели, Max(КоличествоПродаж) as МаксимальноПродаж from test
                GROUP BY ДеньНедели
            )
            Select max_sales.ДеньНедели, test.ЧасДня, МаксимальноПродаж from max_sales
            JOIN test ON МаксимальноПродаж = КоличествоПродаж
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
query = '''Select Номенклатура, Sum(Количество) As Количество from sales
            JOIN products ON products.Ссылка = Номенклатура
            GROUP BY Номенклатура
        '''

df = pd.read_sql_query(query, conn2)
fig = px.histogram(df, x="Номенклатура", y="Количество")
fig.show()

def first_task():
    df = pd.read_sql_query(task2, conn2)
    print(df)

    def getDowInChar(i: float) -> str:
        dow = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
        return dow[int(i)]

    def getTime(i: float) -> str:
        return f"{int(i)}:00"

    to_show = pd.DataFrame({'Максимально продаж': [row[1][2] for row in df.iterrows()],
                        'День недели': [getDowInChar(row[1][0]) for row in df.iterrows()],
                        'Время': [getTime(row[1][1]) for row in df.iterrows()]})

    fig = px.bar(to_show, x="День недели", y="Максимально продаж", text="Время")
    fig.show()

    fig = px.bar(to_show, x="Время", y="Максимально продаж", color="День недели")
    fig.show()

def second_task():
    pass

def third_task():
    pass

conn.close()
