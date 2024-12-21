import json
import logging
from kafka import KafkaConsumer

def create_consumer(topic, bootstrap_servers='localhost:9092', group_id='test-group'):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka consumer: {e}")
        return None

def consume_events(consumer):
    """Consume events from Kafka and process them."""
    if consumer is None:
        logging.error("Consumer is not initialized.")
        return
    for message in consumer:
        try:
            print(f"Received: {message.value}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")