import csv
import json
import logging
import os
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from shared.config import (
    BATCH_SIZE,
    BUFFER_MEMORY,
    KAFKA_BROKER,
    LINGER_MS,
    SAMPLE_SIZE,
    TOPIC_NAME,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | PRODUCER | %(levelname)s | %(message)s"
)


def json_serializer(data: dict[str, Any]) -> bytes:
    return json.dumps(data).encode('utf-8')


CSV_FILE: str = os.getenv("CSV_FILE", "data/nyt-comments-sorted.csv")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=json_serializer,
        batch_size=BATCH_SIZE,
        buffer_memory=BUFFER_MEMORY,
        linger_ms=LINGER_MS,
        retries=3,
        acks=1,
    )

    if SAMPLE_SIZE > 0:
        logging.info(f"Running in SAMPLE MODE: processing first {SAMPLE_SIZE} rows")
    else:
        logging.info("Producer started. Beginning CSV stream...")

    with open(CSV_FILE, newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        for i, row in enumerate(reader):
            if SAMPLE_SIZE > 0 and i >= SAMPLE_SIZE:
                break

            doc = {
                "id": int(row["commentID"]),
                "text": row["commentBody"],
                "user_id": str(row["userID"]),
            }

            try:
                producer.send(
                    TOPIC_NAME,
                    key=doc["user_id"].encode("utf-8"),
                    value=doc
                )

                if i % 1000 == 0:
                    logging.info(f"Sent {i} documents...")

            except Exception as e:
                logging.error(f"Failed to send document id={doc['id']} | error={e}")

    producer.flush()

    if SAMPLE_SIZE > 0:
        logging.info(f"Finished streaming {SAMPLE_SIZE} sampled documents.")
    else:
        logging.info("Finished streaming CSV.")


def wait_for_kafka():
    logging.info("Waiting for Kafka...")

    for i in range(20):
        try:
            KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            logging.info("Kafka is ready!")
            return
        except NoBrokersAvailable:
            logging.warning(f"Kafka not ready yet... retry {i+1}/20")
            time.sleep(2)

    logging.error("Kafka never became available")
    raise Exception("Kafka never became available")


if __name__ == "__main__":
    wait_for_kafka()
    main()
