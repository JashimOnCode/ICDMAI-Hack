import threading
import schedule
import logging
import time
from flask import Flask, request, jsonify
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
MODEL_PATH = 'model/random_forest_model_v1.pkl'
try:
    rf_model = joblib.load(MODEL_PATH)
    logging.info(f"Model loaded successfully from {MODEL_PATH}")
except FileNotFoundError:
    logging.error(f"Model file not found at {MODEL_PATH}")
    rf_model = None

# Kafka producer and consumer
producer = create_producer()
consumer = create_consumer('app_activity')

# Stop event for threads
stop_event = threading.Event()

# Logging setup
logging.basicConfig(level=logging.INFO)

# Kafka Producer Thread
def start_kafka_producer(producer, stop_event):
    logging.info("Starting Kafka producer...")
    schedule.every(1).seconds.do(lambda: produce_events(producer, stop_event))
    while not stop_event.is_set():
        schedule.run_pending()
        time.sleep(1)

# Kafka Consumer Thread
def start_kafka_consumer(consumer):
    logging.info("Starting Kafka consumer...")
    consume_events(consumer)

# Fetch data from MySQL
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
            return events_df
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return pd.DataFrame()

# Flask API Endpoints
@app.route("/predict", methods=["POST"])
def predict():
    """Predict churn based on input data."""
    if not rf_model:
        return jsonify({"error": "Model not loaded"}), 500
    
    try:
        data = request.json
        input_df = pd.DataFrame([data])  # Convert input data to DataFrame
        prediction = rf_model.predict_proba(input_df)[0][1]  # Probability of churn
        return jsonify({"churn_risk": prediction})
    except Exception as e:
        logging.error(f"Prediction error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/fetch-data", methods=["GET"])
def fetch():
    """Fetch data from the database."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not (start_date and end_date):
        return jsonify({"error": "start_date and end_date are required"}), 400
    
    data = fetch_data(start_date, end_date)
    return data.to_json(orient="records")

# Start Kafka threads in daemon mode
def start_threads():
    producer_thread = threading.Thread(target=start_kafka_producer, args=(producer, stop_event), daemon=True)
    consumer_thread = threading.Thread(target=start_kafka_consumer, args=(consumer,), daemon=True)
    producer_thread.start()
    consumer_thread.start()

if __name__ == "__main__":
    try:
        start_threads()
        app.run(host="0.0.0.0", port=5000, debug=True)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        stop_event.set()
