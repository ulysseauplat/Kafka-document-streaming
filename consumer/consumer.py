import gc
import json
import logging
import os
import time
from typing import Any, Optional

from kafka import KafkaConsumer

from database.database import (
    batch_insert_similarities,
    batch_track_doc_users,
    batch_update_user_stats_comments,
    batch_update_user_stats_similarities,
    sync_user_stats_from_doc_users,
    update_consumer_stats,
    update_system_latency,
)
from lsh.lsh_index import LSHIndex
from lsh.minhash import compute_minhash_signature, generate_hash_params
from lsh.preprocess import process_comment
from shared.config import (
    FETCH_MAX_WAIT_MS,
    FETCH_MIN_BYTES,
    KAFKA_BROKER,
    LSH_BANDS,
    MAX_POLL_RECORDS,
    NUM_HASHES,
    PRIME,
    SEEN_PAIRS_MAX_SIZE,
    SHINGLE_DICT_MAX_SIZE,
    SHINGLE_SIZE,
    SIMILARITY_THRESHOLD,
    TOPIC_NAME,
)
from shared.s3_writer import S3Writer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | CONSUMER | %(levelname)s | %(message)s"
)

CONSUMER_ID: int = int(os.getenv("CONSUMER_ID", "1"))

# ---------------------------
# TUNING CONSTANTS
# ---------------------------
COMMIT_INTERVAL = 100
COMMIT_TIME_INTERVAL_SEC = 5

DB_FLUSH_INTERVAL = 100
DB_FLUSH_TIME_SEC = 10
METRICS_FLUSH_INTERVAL = 200
LOG_SAMPLE_INTERVAL = 200


def json_deserializer(data: bytes) -> dict[str, Any]:
    return json.loads(data.decode("utf-8"))


def jaccard(set1: set[int], set2: set[int]) -> float:
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def wait_for_kafka():
    logging.info("Waiting for Kafka...")

    for i in range(30):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME, bootstrap_servers=KAFKA_BROKER, consumer_timeout_ms=2000
            )
            consumer.close()
            logging.info("Kafka is ready!")
            return
        except Exception as e:
            logging.warning(f"Kafka not ready yet ({i + 1}/30): {e}")
            time.sleep(2)

    raise Exception("Kafka never became available")


def main():
    logging.info("Consumer starting...")

    sync_user_stats_from_doc_users()
    s3_writer = S3Writer()

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

    k: int = SHINGLE_SIZE
    n_hash: int = NUM_HASHES
    r: int = LSH_BANDS

    hash_params = generate_hash_params(n_hash, PRIME)
    lsh = LSHIndex(r, n_hash // r)

    shingle_dict: dict[int, set[int]] = {}
    seen_pairs: set[tuple[int, int]] = set()

    # ---------------------------
    # STATE
    # ---------------------------
    processed_count = 0
    similarity_count = 0
    total_latency_ms = 0.0
    min_latency_ms = float('inf')
    max_latency_ms = 0.0
    latency_count = 0

    last_commit_time = time.time()
    last_metrics_time = time.time()
    last_flush_time = time.time()
    last_processed_count = 0
    last_total_latency = 0.0
    last_min_latency = float('inf')
    last_max_latency = 0.0
    last_latency_count = 0

    current_user: Optional[str] = None

    # ---------------------------
    # BATCH BUFFERS
    # ---------------------------
    doc_user_buffer = []
    similarity_buffer = []
    user_stats_buffer = []
    similarity_user_buffer = []

    def flush_db_buffers():
        nonlocal doc_user_buffer, similarity_buffer, user_stats_buffer, similarity_user_buffer

        if doc_user_buffer:
            batch_track_doc_users(doc_user_buffer)

        if similarity_buffer:
            batch_insert_similarities(similarity_buffer, CONSUMER_ID)

        if user_stats_buffer:
            batch_update_user_stats_comments(user_stats_buffer)

        if similarity_user_buffer:
            batch_update_user_stats_similarities(similarity_user_buffer, CONSUMER_ID)

        doc_user_buffer.clear()
        similarity_buffer.clear()
        user_stats_buffer.clear()
        similarity_user_buffer.clear()

    for message in consumer:
        if message is None:
            continue

        msg_start = time.time()
        doc = message.value

        if not doc:
            continue

        doc_id = doc.get("id")
        text = doc.get("text")
        user_id = doc.get("user_id")
        produced_at = doc.get("produced_at")

        # ---------------------------
        # LATENCY TRACKING
        # ---------------------------
        if produced_at:
            latency_ms = (msg_start - produced_at) * 1000
            update_system_latency(latency_ms, 1, latency_ms, latency_ms)

            total_latency_ms += latency_ms
            min_latency_ms = min(min_latency_ms, latency_ms)
            max_latency_ms = max(max_latency_ms, latency_ms)
            latency_count += 1

        # ---------------------------
        # USER SWITCH RESET
        # ---------------------------
        if current_user is not None and user_id != current_user:
            shingle_dict.clear()
            lsh.clear_all()
            seen_pairs.clear()

        current_user = user_id

        # ---------------------------
        # BUFFER DB OPERATIONS
        # ---------------------------
        user_stats_buffer.append(user_id)
        doc_user_buffer.append((doc_id, user_id))

        s3_writer.add(doc, user_id)

        shingles = process_comment(text, k)
        if not shingles:
            continue

        shingle_dict[doc_id] = shingles
        processed_count += 1

        signature = compute_minhash_signature(shingles, hash_params, PRIME)
        lsh.insert(signature, doc_id)

        # ---------------------------
        # LSH SIMILARITY CHECK
        # ---------------------------
        for i, j in lsh.candidate_pairs:
            pair = tuple(sorted((i, j)))

            if pair in seen_pairs:
                continue

            seen_pairs.add(pair)

            if i in shingle_dict and j in shingle_dict:
                sim = jaccard(shingle_dict[i], shingle_dict[j])

                if sim >= SIMILARITY_THRESHOLD:
                    similarity_buffer.append((i, j, sim))
                    similarity_count += 1
                    similarity_user_buffer.append((i, j))

        # ---------------------------
        # LOGGING (throttled)
        # ---------------------------
        if processed_count % LOG_SAMPLE_INTERVAL == 0:
            logging.info(
                f"PROGRESS | processed={processed_count} "
                f"similarities={similarity_count}"
            )

        # ---------------------------
        # FLUSH DB BATCHES (count or time based)
        # ---------------------------
        now = time.time()
        if processed_count % DB_FLUSH_INTERVAL == 0 or now - last_flush_time >= DB_FLUSH_TIME_SEC:
            flush_db_buffers()
            last_flush_time = now

        # ---------------------------
        # UPDATE CONSUMER STATS
        # ---------------------------
        if now - last_metrics_time >= 10:
            interval_processed = processed_count - last_processed_count
            interval_time = now - last_metrics_time
            throughput = interval_processed / interval_time if interval_time > 0 else 0

            interval_latency = total_latency_ms - last_total_latency
            interval_count = latency_count - last_latency_count
            avg_latency = interval_latency / interval_count if interval_count > 0 else 0

            logging.info(f"STATS | consumer={CONSUMER_ID} throughput={throughput:.2f} processed={processed_count} latency={avg_latency:.2f}ms")

            try:
                update_consumer_stats(
                    CONSUMER_ID,
                    throughput,
                    processed_count,
                    avg_latency,
                    last_min_latency if last_min_latency != float('inf') else 0,
                    last_max_latency,
                    interval_latency,
                    interval_count,
                )
            except Exception as e:
                logging.error(f"update_consumer_stats failed: {e}")

            last_metrics_time = now
            last_processed_count = processed_count
            last_total_latency = total_latency_ms
            last_min_latency = min_latency_ms
            last_max_latency = max_latency_ms
            last_latency_count = latency_count

        # ---------------------------
        # COMMIT STRATEGY (FIXED)
        # ---------------------------
        now = time.time()
        if (
            processed_count % COMMIT_INTERVAL == 0
            or now - last_commit_time >= COMMIT_TIME_INTERVAL_SEC
        ):
            try:
                consumer.commit()
                last_commit_time = now
            except Exception as e:
                logging.error(f"Kafka commit failed: {e}")

        # ---------------------------
        # MEMORY CLEANUP (unchanged logic)
        # ---------------------------
        if processed_count % 500 == 0:
            if len(shingle_dict) > SHINGLE_DICT_MAX_SIZE:
                keys_to_remove = sorted(shingle_dict.keys())[: len(shingle_dict) // 4]
                for key in keys_to_remove:
                    del shingle_dict[key]

            if len(seen_pairs) > SEEN_PAIRS_MAX_SIZE:
                pairs_to_remove = list(seen_pairs)[: len(seen_pairs) // 4]
                for p in pairs_to_remove:
                    seen_pairs.discard(p)

            gc.collect()

    # ---------------------------
    # FINAL FLUSH
    # ---------------------------
    flush_db_buffers()
    consumer.commit()

    s3_writer.close()

    logging.info(
        f"Consumer stopped | processed={processed_count} | similarities={similarity_count}"
    )


if __name__ == "__main__":
    wait_for_kafka()
    main()
