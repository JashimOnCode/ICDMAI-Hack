import json
import logging
import time
from kafka import KafkaProducer

def create_producer(bootstrap_servers='localhost:9092'):
    """Create and return a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("Kafka producer created successfully.")
        return producer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka producer: {e}")
        return None

def send_event(producer, topic, event):
    """Send a single event to a Kafka topic."""
    if producer is None:
        logging.error("Producer is not initialized.")
        return
    try:
        producer.send(topic, event)
        logging.info(f"Event sent to topic {topic}: {event}")
    except Exception as e:
        logging.error(f"Failed to send event: {e}")

def produce_events(producer, stop_event, topic='app_activity'):
    """Continuously produce events to Kafka until stop_event is set."""
    if producer is None:
        logging.error("Producer is not initialized.")
        return
    try:
        while not stop_event.is_set():
            event = {
                "customer_id": 1234,
                "event": "app_login",
                "timestamp": time.time()
            }
            send_event(producer, topic, event)
            time.sleep(1)
    except Exception as e:
        logging.error(f"Error in produce_events: {e}")
