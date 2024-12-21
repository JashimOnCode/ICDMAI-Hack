import json
import logging
from kafka import KafkaConsumer

def create_consumer(topic, bootstrap_servers='localhost:9092', group_id='test-group'):
    """Create and return a Kafka consumer."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"Connected to Kafka topic: {topic}")
        return consumer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka consumer: {e}")
        return None

def consume_events(consumer):
    """Consume events from Kafka and process them."""
    if consumer is None:
        logging.error("Consumer is not initialized.")
        return
    try:
        for message in consumer:
            logging.info(f"Received message: {message.value}")
    except Exception as e:
        logging.error(f"Error while consuming messages: {e}")
