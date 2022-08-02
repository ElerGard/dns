import pandas as pd
import plotly.express as px
import psycopg2
from time import time
from sqlalchemy import create_engine

# Добавление файлов в подключенную бд
def inser_csv_to_db(files: list, conn, conn_sqlalchemy) -> None:
    
    # Запросы на создание таблиц
    # Первичные и вторичные ключи не настраивал так как не было чётких критериев на этот счёт
    queries_tables = (
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
    
    # Создаются таблицы если их нет
    cursor = conn.cursor()
    for query in queries_tables:
        cursor.execute(query)
    cursor.close()
    conn.commit()
    
    # На моей системе при довольно большой нагрузке файл t_sales добавляется, в среднем, за 5 минут 
    for filename in files:
        st = time()
        df = pd.read_csv(f't_{filename}.csv', sep=',')
        df = df.rename(columns={'Unnamed: 0': 'Индекс'})
        df.to_sql(filename, conn_sqlalchemy, if_exists='replace', index=False)
        et = time()
        print(f'Файл t_{filename}.csv добавлен в бд за: {et - st}')
       
    print("Добавление файлов в БД завершено")
    return None

# Задание по аналитической части
def first_part(conn_sqlalchemy) -> None:
    
    # Требуется рассчитать и вывести название и количество в порядке убывания. 
    # Также выводил ещё и индексы
    task1 = ( 
        # Десять первых магазинов по количеству продаж;
        '''SELECT branches.Индекс AS ФилиалИД, branches.Наименование, Sum(Количество) AS ТоваровПродано from sales
            JOIN branches ON branches.Ссылка = Филиал 
            WHERE (position('склад' in lower(КраткоеНаименование)) = 0)
            GROUP BY ФилиалИД, branches.Наименование
            ORDER BY ТоваровПродано DESC
            LIMIT 10
        ''',
        # Десять первых складов по количеству продаж;
        # За склады брал только те данные, где в кратком наименовании есть слово склад
        '''SELECT branches.Индекс AS ФилиалИД, branches.Наименование, Sum(Количество) AS ТоваровПродано from sales
            JOIN branches ON branches.Ссылка = Филиал 
            WHERE (position('склад' in lower(КраткоеНаименование)) > 0)
            GROUP BY ФилиалИД, branches.Наименование
            ORDER BY ТоваровПродано DESC
            LIMIT 10
        ''',
        # За товары брал всё что в документе с товарами, отрицательное количество считал как товар который вернули,
        # поэтому прибыли с него не было. Также не было условий на счёт товаров с названиями например "Доставка ..."
        # Они учитываются, но это исправляется дополнением в условие where
        
        # Десять самых продаваемых товаров по складам;
        '''SELECT branches.Индекс AS ФилиалИД, products.Индекс AS ТоварИД, branches.Наименование, 
                products.Наименование AS Товар, Sum(Количество) AS ТоваровПродано from sales
            JOIN products ON products.Ссылка = Номенклатура
            JOIN branches ON branches.Ссылка = Филиал
            WHERE (position('склад' in lower(КраткоеНаименование)) > 0)
            GROUP BY ТоварИД, ФилиалИД, Товар, branches.Наименование
            ORDER BY ТоваровПродано DESC
            LIMIT 10
        ''',
        # Если делать следующий запрос через pgAdmin, то скорость в 5 раз быстрее.
        # Такая разница только на нём и непонятно почему такая реакция. Остальные +- похожи по времени
        
        # Десять самых продаваемых товаров по магазинам;
        '''SELECT branches.Индекс AS ФилиалИД, products.Индекс AS ТоварИД, branches.Наименование, 
                products.Наименование AS Товар, Sum(Количество) AS ТоваровПродано from sales
            JOIN products ON products.Ссылка = Номенклатура
            JOIN branches ON branches.Ссылка = Филиал
            WHERE (position('склад' in lower(КраткоеНаименование)) = 0)
            GROUP BY ТоварИД, ФилиалИД, Товар, branches.Наименование
            ORDER BY ТоваровПродано DESC
            LIMIT 10
        ''',
        # Десять первых магазинов по количеству продаж;
        '''With tmp AS (
                SELECT branches.Город, Sum(Количество) AS ТоваровПродано from sales
                JOIN branches ON branches.Ссылка = Филиал 
                GROUP BY branches.Город
            )
            SELECT cities.Наименование AS НаселенныйПункт, ТоваровПродано from tmp
            JOIN cities ON Город = cities.Ссылка
            ORDER BY ТоваровПродано DESC
            LIMIT 10
        '''
    )

    # Требуется рассчитать и вывести в какие часы и в какой день недели происходит максимальное количество продаж.
    task2 = '''With tmp AS (
                    SELECT EXTRACT (DOW FROM CAST(Период AS timestamp)) AS ДеньНедели,
                            EXTRACT (HOUR FROM CAST(Период AS timestamp)) AS ЧасДня, Sum(Количество) AS ТоваровПродано
                    FROM sales
                    GROUP BY ДеньНедели, ЧасДня
                ), max_sales AS (
                    SELECT ДеньНедели, Max(ТоваровПродано) AS МаксимальноПродаж from tmp
                    GROUP BY ДеньНедели
                )
                SELECT max_sales.ДеньНедели, tmp.ЧасДня, МаксимальноПродаж from max_sales
                JOIN tmp ON МаксимальноПродаж = ТоваровПродано
            '''

    # Немного не понял что именно требуется, поэтому сделал два графика: основанный на данных второго задания и по всем дням и часам
    task3 = '''SELECT EXTRACT (DOW FROM CAST(Период AS timestamp)) AS ДеньНедели,
                        EXTRACT (HOUR FROM CAST(Период AS timestamp)) AS Время, Sum(Количество) AS МаксимальноПродаж
                FROM sales
                GROUP BY ДеньНедели, Время
            '''
    
    # Функция для выполнения заданий по запросам
    def do_task(sql_query: str, i: int) -> pd.DataFrame:
        st = time()
        df = pd.read_sql_query(sql_query, conn_sqlalchemy)
        et = time()
        # Сделал принт df тут для более удобной читаемости при проверке
        print(df)
        print(f"Время выполнения задания {i} - {et - st}\n")
        return df
    
    # Выполнение запросов по первому заданию
    for task in task1:
        do_task(task, 1)
        
    # Выполнение запроса по второму заданию
    df = do_task(task2, 2)
    
    # Переводит числовое значение дня недели из БД в текст. Для удобства чтения на графиках
    def dow_to_char(i: float) -> str:
            dow = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
            return dow[int(i)]

    # Также перевод времени для графиков
    def time_to_char(i: float) -> str:
        return f"{int(i)}:00"

    # Переделываем DataFrame и строим график
    to_show = pd.DataFrame({'МаксимальноПродаж': [row[1][2] for row in df.iterrows()],
                        'ДеньНедели': [dow_to_char(row[1][0]) for row in df.iterrows()],
                        'Время': [time_to_char(row[1][1]) for row in df.iterrows()]})

    fig = px.bar(to_show, x="ДеньНедели", y="МаксимальноПродаж", text="Время")
    fig.show()
    
    # Выполнение запроса по третьему заданию
    df = do_task(task3, 3)

    to_show = pd.DataFrame({"Время": df["Время"].values, 
                            "МаксимальноПродаж": df["МаксимальноПродаж"].values,
                            "День": [dow_to_char(row[1][0]) for row in df.iterrows()]})

    fig = px.bar(to_show, x="Время", y="МаксимальноПродаж", barmode="group", facet_col="День")
    fig.show()
    
    print("Выполнение аналитической части задания завершено")
    return None

# Задание по расчётной части
def second_part(conn, conn_sqlalchemy) -> None:
    products_type = '''SELECT Номенклатура, Sum(Количество) AS ТоваровПродано from sales
                    JOIN products ON products.Ссылка = Номенклатура
                    GROUP BY Номенклатура
                '''
                
    st = time()
    df = pd.read_sql_query(products_type, conn_sqlalchemy)
    
    t = df.quantile([0.3, 0.9])

    def getProductType(i: float) -> str:
        q1 = t['ТоваровПродано'].values[0]
        q2 = t['ТоваровПродано'].values[1]
        if (i < q1):
            return "Наименее продаваемый"
        if (i >= q1 and i <= q2):
            return "Средне продаваемый"
        if (i > q2):
            return "Наиболее продаваемый"

    to_save = pd.DataFrame({'Номенклатура': [row[1][0] for row in df.iterrows()],
                        'КлассТовара': [getProductType(row[1][1]) for row in df.iterrows()]})

    query = '''CREATE TABLE IF NOT EXISTS type_products (
                    Индекс int, 
                    Ссылка varchar(60),
                    Наименование varchar(60),
                    Город varchar(60),
                    КраткоеНаименование varchar(60),
                    Регион varchar(60)
                );
            '''
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    conn.commit()

    to_save.to_sql('type_products', conn_sqlalchemy, if_exists='replace', index=False)
    et = time()
    print(to_save)
    print(f"Время выполнения задания расчётной части - {et - st}\n")
    print("Выполнение расчётной части задания завершено")
    return None


def main():
    # Подключение к базе данных

    try:
        conn = psycopg2.connect(
            host='localhost',
            database='dns',
            user='postgres',
            password='1111'
        )
        
        db = create_engine('postgresql://postgres:1111@localhost/dns')
        conn_sqlalchemy = db.connect()
        
    except psycopg2.OperationalError:
        print("Не удалось подключиться к базе данных")
        exit()
    
    filename= ['products', 'cities', 'branches', 'sales']
    
    inser_csv_to_db(filename, conn, conn_sqlalchemy)
    first_part(conn_sqlalchemy)
    second_part(conn, conn_sqlalchemy)
    conn.close()
    conn_sqlalchemy.close()
    print("Работа завершена")
    
main()