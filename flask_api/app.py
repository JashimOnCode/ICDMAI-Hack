import threading
import concurrent.futures
import schedule
import logging
import time
from flask import Flask
from flask_cors import CORS
import joblib
import os
import pandas as pd
import mysql.connector
from pipeline.kafka_producer import create_producer, produce_events
from pipeline.kafka_consumer import create_consumer, consume_events

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Load the trained model
rf_model = joblib.load('models/random_forest_model_v1.pkl')

logging.basicConfig(level=logging.INFO)

def start_kafka_producer(producer, stop_event):
    schedule.every(1).seconds.do(lambda: produce_events(producer, stop_event))
    while not stop_event.is_set():
        schedule.run_pending()
        time.sleep(1)

def start_kafka_consumer(consumer):
    consume_events(consumer)

# Initialize Kafka producer and consumer
producer = create_producer()
consumer = create_consumer('app_activity')

stop_event = threading.Event()

# Use ThreadPoolExecutor to manage concurrent execution of producer and consumer
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.submit(start_kafka_producer, producer, stop_event)
    executor.submit(start_kafka_consumer, consumer)

# To stop the threads gracefully
stop_event.set()

def fetch_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch data from the database within the specified date range."""
    try:
        with mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', '951Kdroot@12'),
            database=os.getenv('DB_NAME', 'MLhack')
        ) as connection:
            query = """
            SELECT 
                c.customer_id,
                c.age,
                c.tenure,
                c.monthly_usage,
                SUM(CASE WHEN e.event_type = 'Complaints' THEN 1 ELSE 0 END) AS complaints,
                SUM(CASE WHEN e.event_type = 'Returns' THEN 1 ELSE 0 END) AS returns,
                SUM(CASE WHEN e.event_type = 'email_open' THEN 1 ELSE 0 END) AS emails_opened,
                SUM(CASE WHEN e.event_type = 'login' THEN 1 ELSE 0 END) AS daily_logins,
                SUM(CASE WHEN e.event_type = 'sensor_trigger' THEN 1 ELSE 0 END) AS sensor_triggers
            FROM events e
            JOIN customers c ON e.customer_id = c.customer_id
            WHERE e.event_timestamp >= %(start_date)s AND e.event_timestamp < %(end_date)s
            GROUP BY c.customer_id, c.age, c.tenure, c.monthly_usage;
            """
            events_df = pd.read_sql(query, connection, params={'start_date': start_date, 'end_date': end_date})
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
        return pd.DataFrame()
    return events_df