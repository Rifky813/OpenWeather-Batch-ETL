import psycopg2
from psycopg2 import sql
import json
import os

db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_name = os.getenv('POSTGRES_DB')
db_host = os.getenv('DB_HOST', 'postgres')
db_port = os.getenv('DB_PORT', '5432')

if not all([db_user, db_password, db_name]):
    raise ValueError("WARNING: Set the Database Credential first!")

DB_CONFIG = {
    "dbname": db_name,
    "user": db_user,
    "password": db_password,
    "host": db_host,
    "port": db_port
}

def create_table_if_not_exists(cursor):
    try:
        with open('sql/create_table.sql', 'r') as f:
            create_query = f.read()
    
        cursor.execute(create_query)
        print('Table created.')
    except FileNotFoundError:
        raise Exception(f'SQL file not found: sql/create_table.sql')


def load_data(data):
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        create_table_if_not_exists(cur)

        insert_query = """
        INSERT INTO weather_data (
            city, country, weather_main,
            weather_description, temp, feels_like, pressure, 
            humidity, wind_speed, timestamp
        ) VALUES (
            %(city)s, %(country)s, %(weather_main)s,
            %(weather_description)s, %(temp)s, %(feels_like)s, %(pressure)s,
            %(humidity)s, %(wind_speed)s, %(timestamp)s
        )
        """

        cur.execute(insert_query, data)
        conn.commit()

        print(f"Done! {data['city']}'s weather data stored in database.")
    
    except Exception as e:
        print(f'Fatal Error: {e}')
        raise e
    
    finally:
        if conn:
            conn.close()
    

if __name__ == '__main__':
    mock_data = {
        'city': 'Bekasi',
        'country': 'ID',
        'latitude': -6.2349,
        'longitude': 106.9896,
        'weather_main': 'Clouds',
        'weather_description': 'few clouds',
        'temp': 27.03,
        'feels_like': 29.62,
        'pressure': 1010,
        'humidity': 78,
        'wind_speed': 5.14,
        'timestamp': '2025-12-18 18:00:00'
    }

    load_data(mock_data)