# producer/producer.py

from kafka import KafkaProducer
import json
import time

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # the local port where the communications are happening
TOPIC_NAME = 'documents' #name of the channel where the messages will go

# Test document to see if everything works
documents = [
    {"id": 1, "text": "This is the first document."},
    {"id": 2, "text": "Here is the second document."},
    {"id": 3, "text": "And a third one."},
]

def json_serializer(data):  # Kafka only sends bytes so we have to convert everything first
    return json.dumps(data).encode('utf-8')

def main():
    # Create producer and we set it up with our parameters
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=json_serializer
    )

    print(f"Sending {len(documents)} documents to topic '{TOPIC_NAME}'...")
    
    for doc in documents:
        producer.send(TOPIC_NAME, doc)
        print(f"Sent: {doc}")
        time.sleep(1)  # small delay for demonstration

    # # Ensure all queued messages are delivered to Kafka before exiting
    producer.flush()
    print("All documents sent!")

if __name__ == "__main__":
    main()
