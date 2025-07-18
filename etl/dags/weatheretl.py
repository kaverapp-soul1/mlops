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
    dag_id="wind_etl",
    default_args=default_args,
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["iot", "thingspeak", "etl","postgres"],
    description="A DAG to extract wind data from ThingSpeak (current or historical) and load it into PostgreSQL",
) as dag:

    @task
    def extract_wind(**kwargs):
        http_hook=HttpHook(method="GET",http_conn_id=API_CON_ID)

        # Check if custom date range is provided in DAG config
        dag_run = kwargs.get('dag_run')
        conf = dag_run.conf if dag_run else {}
        
        # Build endpoint with optional date parameters
        endpoint = "channels/1785844/feeds.json"
        
        # Add date parameters if provided
        params = []
        if conf.get('start_date'):
            # Convert date to ISO format with timezone
            start_date = conf['start_date']
            if 'T' not in start_date:
                start_date += 'T00:00:00Z'
            params.append(f"start={start_date}")
        if conf.get('end_date'):
            # Convert date to ISO format with timezone
            end_date = conf['end_date']
            if 'T' not in end_date:
                end_date += 'T23:59:59Z'
            params.append(f"end={end_date}")
        if conf.get('results'):
            params.append(f"results={conf['results']}")
        else:
            # Default to get more results if no specific number requested
            params.append("results=8000")
        
        if params:
            endpoint += "?" + "&".join(params)
        
        print(f"Fetching data from: {endpoint}")
        response=http_hook.run(endpoint=endpoint)

        if response.status_code != 200:
            # Raise an exception if the API call fails
            raise Exception(f"Failed to fetch data from API: {response.status_code} - {response.text}")
        
        # Only parse JSON if the request was successful
        data = response.json()
        feeds_count = len(data.get('feeds', []))
        print(f"Extracted {feeds_count} records from API")
        
        if feeds_count == 0:
            print("WARNING: No data returned from API")
        else:
            # Show date range of fetched data
            feeds = data.get('feeds', [])
            if feeds:
                first_date = feeds[0].get('created_at', 'Unknown')
                last_date = feeds[-1].get('created_at', 'Unknown')
                print(f"Data range: {first_date} to {last_date}")
        
        return data

        if response.status_code != 200:
            # Raise an exception if the API call fails
            raise Exception(f"Failed to fetch data from API: {response.status_code} - {response.text}")
        
        # Only parse JSON if the request was successful
        data = response.json()
        print(f"Extracted {len(data.get('feeds', []))} records from API")
        return data
    
    @task
    def transform_data(wind_data):
        transformed_entries = []

        for entry in wind_data["feeds"]:
            transformed_entry = {
                "entry_id": entry["entry_id"],
                "created_at": datetime.fromisoformat(entry["created_at"].replace("Z", "+00:00")), # Convert to datetime object
                "wind_speed": float(entry["field1"]) if entry["field1"] else None,
                "wind_power": float(entry["field3"]) if entry["field3"] else None,
                "air_density": float(entry["field4"]) if entry["field4"] else None,
                "temperature_f": float(entry["field5"]) if entry["field5"] else None,
                "pressure_mmhg": float(entry["field6"]) if entry["field6"] else None
            }
            transformed_entries.append(transformed_entry)

        print(f"Transformed {len(transformed_entries)} records")
        return transformed_entries
    
    @task
    def load_data(transformed_data):
        postgres_hook=PostgresHook(postgres_conn_id=POSTGRES_CON_ID)

        conn=None
        cursor=None

        try:
            conn=postgres_hook.get_conn()
            cursor=conn.cursor()

            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wind_data (
                    id SERIAL PRIMARY KEY,
                    entry_id INT UNIQUE,
                    created_at TIMESTAMP,
                    wind_speed FLOAT,
                    wind_power FLOAT,
                    air_density FLOAT,
                    temperature_f FLOAT,
                    pressure_mmhg FLOAT
                )
            """)

            # Use ON CONFLICT DO UPDATE to handle duplicates
            insert_query = """
                INSERT INTO wind_data (entry_id, created_at, wind_speed, wind_power, air_density, temperature_f, pressure_mmhg)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (entry_id) DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_power = EXCLUDED.wind_power,
                    air_density = EXCLUDED.air_density,
                    temperature_f = EXCLUDED.temperature_f,
                    pressure_mmhg = EXCLUDED.pressure_mmhg
            """
            
            inserted_count = 0
            updated_count = 0
            
            for record in transformed_data:
                cursor.execute(insert_query, (
                    record["entry_id"],
                    record["created_at"],
                    record["wind_speed"],
                    record["wind_power"],
                    record["air_density"],
                    record["temperature_f"],
                    record["pressure_mmhg"]
                ))
                
                # Check if this was an insert or update
                if cursor.rowcount > 0:
                    inserted_count += 1

            conn.commit()
            print(f"Successfully processed {len(transformed_data)} records")
            print(f"Database operations completed")
            
        except Exception as e:
            print(f"Error loading data: {e}")
            if conn:
                conn.rollback() # Rollback on error
            raise  # Re-raise the exception so Airflow knows the task failed
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    wind_data = extract_wind()
    transformed_data = transform_data(wind_data)    
    load_data(transformed_data)