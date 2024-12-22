import threading
import schedule
import logging
import time
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import joblib
import os
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
import mysql.connector
import kafka_producer as kp
import kafka_consumer as kc
import spark_processing as sp

# Logging setup
logging.basicConfig(level=logging.INFO)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Load the trained model
MODEL_PATH = 'model/random_forest_model_v1.pkl'
try:
    rf_model = joblib.load(MODEL_PATH)
    logging.info(f"Model loaded successfully from {MODEL_PATH}")
except FileNotFoundError:
    logging.error(f"Model file not found at {MODEL_PATH}")
    rf_model = None

# Kafka producer and consumer
producer = kp.create_producer()
consumer = kc.create_consumer('app_activity')

# Stop event for threads
stop_event = threading.Event()

# Kafka Producer Thread
def start_kafka_producer(producer, stop_event):
    logging.info("Starting Kafka producer...")
    schedule.every(1).seconds.do(lambda: kp.produce_events(producer, stop_event))
    while not stop_event.is_set():
        schedule.run_pending()
        time.sleep(1)

# Kafka Consumer Thread
def start_kafka_consumer(consumer):
    logging.info("Starting Kafka consumer...")
    kc.consume_events(consumer)

# Spark Stream Processing Thread
def start_spark_processing():
    logging.info("Starting Spark stream processing...")
    sp.process_stream()

# Function to fetch data from the database and make predictions
def fetch_and_predict(start_date, end_date):
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

        # Data preprocessing
        imputer = SimpleImputer(strategy='mean')
        scaler = StandardScaler()

        features = ['age', 'tenure', 'monthly_usage', 'complaints', 'returns',
                    'emails_opened', 'daily_logins', 'sensor_triggers']

        events_df[features] = imputer.fit_transform(events_df[features])
        events_df[features] = scaler.fit_transform(events_df[features])

        events_df = events_df.rename(columns={
            'age': 'Age',
            'tenure': 'Tenure',
            'monthly_usage': 'Monthly_Usage',
            'complaints': 'Complaints',
            'returns': 'Returns',
            'emails_opened': 'Emails_Opened',
            'daily_logins': 'Daily_Logins',
            'sensor_triggers': 'Sensor_Triggers'
        })

        # Make predictions
        X_predict = events_df[['Age', 'Tenure', 'Monthly_Usage', 'Complaints',
                               'Returns', 'Emails_Opened', 'Daily_Logins', 'Sensor_Triggers']]
        events_df['Churn_Prediction'] = rf_model.predict(X_predict)

        # Return only at-risk customers
        at_risk_customers = events_df[events_df['Churn_Prediction'] == 1]
        return at_risk_customers[['customer_id']].to_dict(orient='records')

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return []

# Route for the homepage
@app.route('/')
def index():
    return render_template('index.html')

# Route for predictions
@app.route('/predict', methods=['POST'])
def predict():
    if not rf_model:
        return jsonify({"error": "Model not loaded"}), 500

    month_to_date = {
        'January': ('2024-01-01', '2024-02-01'),
        'February': ('2024-02-01', '2024-03-01'),
        'March': ('2024-03-01', '2024-04-01'),
        'April': ('2024-04-01', '2024-05-01'),
        'May': ('2024-05-01', '2024-06-01'),
        'June': ('2024-06-01', '2024-07-01'),
        'July': ('2024-07-01', '2024-08-01'),
        'August': ('2024-08-01', '2024-09-01'),
        'September': ('2024-09-01', '2024-10-01'),
        'October': ('2024-10-01', '2024-11-01'),
        'November': ('2024-11-01', '2024-12-01'),
        'December': ('2024-12-01', '2025-01-01')
    }

    month = request.json.get('month')
    start_date, end_date = month_to_date.get(month, (None, None))

    at_risk_customers = fetch_and_predict(start_date, end_date)
    return jsonify(at_risk_customers)

# Start Kafka and Spark threads in daemon mode
def start_threads():
    producer_thread = threading.Thread(target=start_kafka_producer, args=(producer, stop_event), daemon=True)
    consumer_thread = threading.Thread(target=start_kafka_consumer, args=(consumer,), daemon=True)
    spark_thread = threading.Thread(target=start_spark_processing, daemon=True)

    producer_thread.start()
    consumer_thread.start()
    spark_thread.start()

if __name__ == "__main__":
    try:
        start_threads()
        app.run(host="0.0.0.0", port=5000, debug=True)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        stop_event.set()