import numpy as np
import pandas as pd
import plotly
import psycopg2
import time

try:
    f = open('db_settings.txt', 'r')
    db_setting = {}
    for st in f:
        data = st.replace(" ", "").replace("\n", "").lower().split('=')
        db_setting[data[0]] = data[1]

except OSError:
    print(f"Файл с настройками базы данных не найден. Установлены настройки по умолчанию")
    db_setting = {'host':'localhost', 'database': 'dns', 'user': 'postgres', 'password': '1111'}
    print(db_setting)
    
try:
    conn = psycopg2.connect(
        host=db_setting['host'],
        database=db_setting['database'],
        user=db_setting['user'],
        password=db_setting['password'])
    
except psycopg2.OperationalError:
    print("Не удалось подключиться к базе данных. Проверьте файл настроек")
    exit()

# Не совсем понял как отличать магазины и склады. 
# За склады считал все строки где есть слово склад в Наименовании или Кратком наименовании, остально как магазины
st = time.time()
df_sales = pd.read_csv('t_sales.csv', sep=",")
et = time.time()
elapsed_time = et - st
print('Execution time:', elapsed_time, 'seconds')
df_sales = df_sales.rename(columns={'Unnamed: 0': 'index', 
                                    "Период": 'period', 
                                    "Филиал": "link",
                                    'Номенклатура': 'nomenclature', 
                                    'Количество': 'amount', 
                                    'Продажа': 'sales'})

df_branches = pd.read_csv('t_branches.csv', sep=",", encoding='utf-8')
df_branches = df_branches.rename(columns={'Unnamed: 0': 'index', 
                                          "Ссылка": 'link', 
                                          "Наименование": "name", 
                                          'Город': 'city', 
                                          'КраткоеНаименование': 'short_name', 
                                          'Регион': 'region'})

df_branches.short_name = df_branches.short_name.fillna("") # для того чтобы не получить exception при обработке
df_storages = df_branches[df_branches.short_name.str.contains("склад", case=False)]
df_storages_sales = df_sales \
                        .groupby(['link'], as_index=False) \
                        .aggregate({'amount': 'sum'}) \
                            
df_storages_sales = df_storages \
                        .merge(df_storages_sales, how='inner', on='link') \
                        .sort_values('amount', ascending=False) 
                        
print(df_storages_sales)
print(df_storages)


def first_task():
    pass

def second_task():
    pass

def third_task():
    pass
