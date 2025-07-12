from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import json

POSTGRES_CON_ID = "postgres_default"
API_CON_ID = "things_speakAPI_default"

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["iot", "thingspeak", "etl","postgres"],
    description="A DAG to extract wind data from ThingSpeak wind data  and load it into PostgreSQL",
) as dag:

    @task
    def extract_wind():
        http_hook=HttpHook(method="GET",http_conn_id=API_CON_ID)

        endpoint="channels/1785844/feeds.json?results=100"
        response=http_hook.run(endpoint=endpoint)

        if response.status_code!=200:
            raise Exception(f"Failed to fetch data from API: {response.status_code}")
        
        data = response.json()
        return data
    
    @task
    def transform_data(wind_data):
        transformed_entries = []

        for entry in wind_data["feeds"]:
            transformed_entry = {
                "entry_id": entry["entry_id"],
                "created_at": entry["created_at"],
                "wind_speed": float(entry["field1"]) if entry["field1"] else None,
                "wind_power": float(entry["field3"]) if entry["field3"] else None,
                "air_density": float(entry["field4"]) if entry["field4"] else None,
                "temperature_f": float(entry["field5"]) if entry["field5"] else None,
                "pressure_mmhg": float(entry["field6"]) if entry["field6"] else None
            }
            transformed_entries.append(transformed_entry)

        return transformed_entries
    
    @task
    def load_data(transformed_data):
        postgres_hook=PostgresHook(postgres_conn_id=POSTGRES_CON_ID)

        conn=None
        cursor=None

        try:
            conn=postgres_hook.get_conn()
            cursor=conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wind_data (
                    entry_id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP,
                    wind_speed FLOAT,
                    wind_power FLOAT,
                    air_density FLOAT,
                    temperature_f FLOAT,
                    pressure_mmhg FLOAT
                )
            """)

            insert_query = """
                INSERT INTO wind_data (entry_id, created_at, wind_speed, wind_power, air_density, temperature_f, pressure_mmhg)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            record_time=None
            for record in transformed_data:
                if record_time is None:
                    record_time = record["created_at"]
                cursor.execute(insert_query, (
                    record["entry_id"],
                    record["created_at"],
                    record["wind_speed"],
                    record["wind_power"],
                    record["air_density"],
                    record["temperature_f"],
                    record["pressure_mmhg"]
                ))

            conn.commit()
        except Exception as e:
            print(f"Error: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    wind_data = extract_wind()
    transformed_data = transform_data(wind_data)  
    load_data(transformed_data)   