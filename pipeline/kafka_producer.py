import json
import logging
import time
from kafka import KafkaProducer

def create_producer(bootstrap_servers='localhost:9092'):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka producer: {e}")
        return None

def send_event(producer, topic, event):
    try:
        producer.send(topic, event)
    except Exception as e:
        logging.error(f"Failed to send event: {e}")

def produce_events(producer, stop_event, topic='app_activity'):
    """Continuously produce events to Kafka."""
    if producer is None:
        logging.error("Producer is not initialized.")
        return
    while not stop_event.is_set():
        event = {
            "customer_id": 1234,
            "event": "app_login",
            "timestamp": time.time()
        }
        send_event(producer, topic, event)
        time.sleep(1)