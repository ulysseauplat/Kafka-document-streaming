# consumer/consumer.py

from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # local port for the communications
TOPIC_NAME = 'documents'

def json_deserializer(data):        #data arrives as bytes so we have to decode it first
    return json.loads(data.decode('utf-8'))

def main():
    # Create Kafka consumer with our parameters
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',  # start from the beginning if no offset
        enable_auto_commit=True,       # commit offsets automatically
        group_id='document-consumer-group',
        value_deserializer=json_deserializer
    )

    print(f"Listening for messages on topic '{TOPIC_NAME}'...\n")
    
    for message in consumer:
        # message.value is already deserialized as dict
        print(f"Received document: {message.value}")

if __name__ == "__main__":
    main()
