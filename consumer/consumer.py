from kafka import KafkaConsumer
import json
import time
import logging
import gc

from lsh.preprocess import process_comment
from lsh.minhash import generate_hash_params, compute_minhash_signature
from lsh.lsh_index import LSHIndex
from shared.config import (
    KAFKA_BROKER, TOPIC_NAME, SHINGLE_SIZE, NUM_HASHES, LSH_BANDS,
    SIMILARITY_THRESHOLD, PRIME, MAX_POLL_RECORDS, FETCH_MIN_BYTES,
    FETCH_MAX_WAIT_MS, SHINGLE_DICT_MAX_SIZE, SEEN_PAIRS_MAX_SIZE
)
from database.database import init_db, insert_similarity


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | CONSUMER | %(levelname)s | %(message)s"
)


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


def jaccard(set1, set2):
    if not set1 or not set2:
        return 0.0
    return len(set1 & set2) / len(set1 | set2)


def wait_for_kafka():
    logging.info("Waiting for Kafka...")

    for i in range(30):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                consumer_timeout_ms=2000
            )
            consumer.close()
            logging.info("Kafka is ready!")
            return
        except Exception as e:
            logging.warning(f"Kafka not ready yet ({i+1}/30): {e}")
            time.sleep(2)

    raise Exception("Kafka never became available")


def main():
    logging.info("Consumer starting...")

    init_db()

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="document-consumer-group",
        value_deserializer=json_deserializer,
        max_poll_records=MAX_POLL_RECORDS,
        fetch_min_bytes=FETCH_MIN_BYTES,
        fetch_max_wait_ms=FETCH_MAX_WAIT_MS,
        max_partition_fetch_bytes=1048576,
    )

    logging.info("Listening for messages...")

    # LSH config
    k = SHINGLE_SIZE
    n_hash = NUM_HASHES
    r = LSH_BANDS

    hash_params = generate_hash_params(n_hash, PRIME)
    lsh = LSHIndex(r, n_hash // r)

    shingle_dict = {}
    seen_pairs = set()

    processed_count = 0
    similarity_count = 0
    cleanup_counter = 0

    start_time = time.time()
    last_log_time = time.time()
    last_processed = 0
    gc_interval = 500

    for message in consumer:

        if message is None:
            continue

        msg_start = time.time()

        doc = message.value
        if not doc:
            continue

        doc_id = doc.get("id")
        text = doc.get("text")

        logging.info(f"Received doc id={doc_id}")

        shingles = process_comment(text, k)
        if not shingles:
            logging.warning(f"Empty shingles for doc id={doc_id}")
            continue

        shingle_dict[doc_id] = shingles
        processed_count += 1

        signature = compute_minhash_signature(shingles, hash_params, PRIME)
        lsh.insert(signature, doc_id)

        # similarity check
        for i, j in lsh.candidate_pairs:
            pair = tuple(sorted((i, j)))

            if pair in seen_pairs:
                continue

            seen_pairs.add(pair)

            if i in shingle_dict and j in shingle_dict:
                sim = jaccard(shingle_dict[i], shingle_dict[j])

                if sim >= SIMILARITY_THRESHOLD:
                    try:
                        insert_similarity(i, j, sim)
                        similarity_count += 1

                        logging.info(
                            f"SIMILARITY FOUND: {i}-{j} = {sim:.2f}"
                        )

                    except Exception as e:
                        logging.error(f"DB insert failed {i}-{j}: {e}")

        # MESSAGE LATENCY
        msg_duration = time.time() - msg_start

        if msg_duration > 1.0:
            logging.warning(f"SLOW MESSAGE id={doc_id} took {msg_duration:.3f}s")

        # PERIODIC METRICS (every 10 sec)
        now = time.time()

        if now - last_log_time >= 10:
            interval_processed = processed_count - last_processed
            interval_time = now - last_log_time

            throughput = (
                interval_processed / interval_time
                if interval_time > 0 else 0
            )

            similarity_rate = (
                (similarity_count / processed_count) * 100
                if processed_count > 0 else 0
            )

            logging.info(
                f"METRICS | throughput={throughput:.2f} msg/s | "
                f"processed={processed_count} | "
                f"similarities={similarity_count} | "
                f"similarity_rate={similarity_rate:.2f}%"
            )

            last_log_time = now
            last_processed = processed_count

        cleanup_counter += 1
        if cleanup_counter >= gc_interval:
            cleanup_counter = 0

            if len(shingle_dict) > SHINGLE_DICT_MAX_SIZE:
                keys_to_remove = sorted(shingle_dict.keys())[:len(shingle_dict) // 4]
                for k in keys_to_remove:
                    del shingle_dict[k]

            if len(seen_pairs) > SEEN_PAIRS_MAX_SIZE:
                pairs_to_remove = list(seen_pairs)[:len(seen_pairs) // 4]
                for p in pairs_to_remove:
                    seen_pairs.discard(p)

            gc.collect()
            logging.info(f"MEMORY CLEANUP | shingle_dict size={len(shingle_dict)} | seen_pairs size={len(seen_pairs)}")

        consumer.commit()
    # FINAL SUMMARY
    total_time = time.time() - start_time


    logging.info(
        f"Consumer stopped | "
        f"processed={processed_count} | "
        f"similarities={similarity_count} | "
        f"runtime={total_time:.2f}s | "
        f"avg_throughput={processed_count/total_time:.2f} msg/s"
    )


if __name__ == "__main__":
    wait_for_kafka()
    main()
