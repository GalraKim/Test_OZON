# Импортируем все нужные библиотеки
import json
import psycopg2
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# Настраиваем логирование на уровень INFO
LOG_LEVEL = "INFO"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
if not isinstance(numeric_level, int):
    numeric_level = logging.INFO
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "data_loader.log"),
    level=numeric_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Загружаем переменные окружения .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logging.info("Database credentials was loaded from .env")
else:
    logging.error(".env file not found.")

# Достаем креды для подключения к бд (В моем случае я использовала PostgreSQL развернутый локально)
DB_USER = os.getenv('DB_USER', 'postgres')  # Добавлены префикс DB_ и значение по умолчанию
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = "ozon_orders" # Указываем имя бд которая будет содержать таблички

# Здесь указываем путь к файлу который нужно загрузить в таблицу ozon_orders
JSON_FILE = 'ozon_orders.json'

# Подсоединяемся к бд, у меня без указания конкретной бд, просто к постгресу
# Через этот объект будем проводить все манипуляции по загрузке, дедупликации
def db_connect():
    db_params = {
        'user': DB_USER,
        'password': DB_PASSWORD,
        'host': DB_HOST,
        'port': DB_PORT,
        'dbname': DB_NAME
    }
    logging.info(f"Connecting to {DB_HOST}:{DB_PORT}")
    client = psycopg2.connect(**db_params)
    return client

# Проверяем, нет ли уже созданной БД, если нет то будем создавать
def is_database_exist(client):
    try:
        client.autocommit = True
        cursor = client.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'")
        exists = cursor.fetchone() # Ловим результат запроса выше
        if not exists:
            logging.info(f"Database {DB_NAME} does not exist. Creating...")
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            logging.info(f"Database {DB_NAME} created successfully")
        else:
            logging.info(f"Database {DB_NAME} already exists")
    except Exception as e: # Если что то пошло не так на проверке ловим ошибки
        logging.error(f"Error checking/creating database: {str(e)}")
        raise

# Аналогично проверяем есть ли уже табличка которую хотели создать (orders)
def is_table_exist(client):
    try:
        # Connect to the database
        cursor = client.cursor()
        table_name = "orders" # Если будет нужно изменить таблицу - меняем тут
        cursor.execute(f"""             
            SELECT EXISTS (                 
                SELECT FROM information_schema.tables                 
                WHERE table_name = '{table_name}'             
            )         
        """)
        table_exists = cursor.fetchone()[0] # Ловим результат по проверке таблицы
        # Если таблички нет то задаем структуру и создаем
        if not table_exists:
            logging.info("Table 'orders' does not exist. Creating...")
            cursor.execute('''             
            CREATE TABLE orders (                 
                order_id TEXT PRIMARY KEY,                 
                status TEXT,                 
                date DATE,                 
                amount REAL,                 
                customer_region TEXT             
            )             
            ''')
            client.commit()
            logging.info("Table 'orders' created successfully")
        else:
            logging.info("Table 'orders' already exists")
    except Exception as e: # если что то еще
        logging.error(f"Error checking/creating table: {str(e)}")
        raise

# Проверяем валидность json и возвращаем распаршеный файл
def parsing_json(file_path):
    try:
        logging.info(f"Trying to load data from file {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logging.info(f"Data loaded successfully. Count rows: {len(data)}")
        return data
    except FileNotFoundError: # Если файл не нашли
        logging.error(f"File {file_path} not found")
        raise
    except json.JSONDecodeError: # Если не получилось распарсить
        logging.error(f"JSON decoding error in file {file_path}")
        raise
    except Exception as e: # если что то еще
        logging.error(f"Unexpected error loading data: {str(e)}")
        raise

# Форматирование даты-датывремени
def format_date_value(date_str):
    try:
        # По заданию нужно было привести в порядок формат даты, я решила что подходящий формат - короткая дата (0.0.0000)
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        return date_obj.strftime("%Y-%m-%d")  # А теперь форматируем чтобы Постгр смог ее загрузить
    except ValueError:  # если что то пойдет не так
        logging.warning(f"Cannot convert date: {date_str}. Using original string.")
        return date_str

# Функция для проверки строки на дубль в бд
def is_duplicated(cursor, order_id):
    cursor.execute("SELECT 1 FROM orders WHERE order_id = %s", (order_id,))
    return cursor.fetchone() is not None

# Основная функция с обработкой и загрузкой данных
def transform_data(data, client):
    try:
        # Connect to the database
        cursor = client.cursor()
        processed_count = 0
        skipped_duplicates = 0
        # проходим по каждой распашеной строчке
        for order in data:
            order_id = order.get('order_id')
            # Если такая запись уже есть то просто скипаем,
            # можно было бы реализовать запись поверх предыдущей если бы данные имели свойство изменяться какими то полями,
            # но оставаться такими же по дате и айди, но я решила придерживаться тз в этот раз
            if is_duplicated(cursor, order_id):
                logging.info(f"Order with ID {order_id} already exists in DB. Skipping.")
                skipped_duplicates += 1
                continue
            # Разворачиваем вложенную структуру
            status = order.get('status')
            date = format_date_value(order.get('date'))
            amount = order.get('amount')
            customer = order.get('customer', {})
            customer_region = customer.get('region')
            # Загружаем данные в таблицу
            cursor.execute(
                "INSERT INTO orders (order_id, status, date, amount, customer_region) VALUES (%s, %s, %s, %s, %s)",
                (order_id, status, date, amount, customer_region)
            )
            processed_count += 1
            # Шагом в 5 записей (т.к. файл пока маленький) логируем загрузку
            if processed_count % 1 == 0:
                logging.info(f"Processed {processed_count} records")
        client.commit() # Сохраняем наш результат
        logging.info(
            f"Data successfully loaded into DB. Processed records: {processed_count}. Skipped duplicates: {skipped_duplicates}")
        return processed_count, skipped_duplicates
    except Exception as e:
        logging.error(f"Error processing and saving data: {str(e)}")
        raise

def main():
    client = db_connect()
    try:
        logging.info("Starting data loading process")
        # Проверяем на существование бд и таблицы
        is_database_exist(client)
        is_table_exist(client)
        # парсим данные
        data = parsing_json(JSON_FILE)
        # пушим их в бд после обработки
        processed_count, skipped_duplicates = transform_data(data, client)
        logging.info(f"Data loading process completed successfully. "                      
                    f"Processed: {processed_count}, Skipped duplicates: {skipped_duplicates}")
        print(f"Data successfully loaded into DB. "               
             f"Processed: {processed_count}, Skipped duplicates: {skipped_duplicates}")
    except Exception as e:
        logging.critical(f"Critical error in data loading process: {str(e)}")
        print(f"Error: {str(e)}")
    client.close()
    logging.info("DB connection closed") # Отсоединяемся от бд

if __name__ == "__main__":
    main()

"""
А эта часть для проверяющих мой код, мне бы хотелось приподнять тебе настроение, поэтому держи старый, но добрый анекдот :)

Гуляют две зубочистки по лесу. Вдруг мимо пробегает ежик.
Одна зубочистка говорит другой:
- Слушай, а ты знала, что здесь ходит автобус?
"""