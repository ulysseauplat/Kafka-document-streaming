import json
import logging
import threading
import time
from collections import defaultdict
from typing import Any, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from shared.s3_config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET,
    S3_FLUSH_INTERVAL,
)

BUCKET_REGION = AWS_REGION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | S3_WRITER | %(levelname)s | %(message)s"
)


class S3Writer:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
        self.s3_client: Optional[Any] = None
        self.bucket = S3_BUCKET
        self.flush_interval = S3_FLUSH_INTERVAL
        self.buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.lock = threading.Lock()
        self.running = True

        if self.enabled:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION,
                config=Config(connect_timeout=5, read_timeout=30, retries={'max_attempts': 3})
            )

            self._ensure_bucket_exists()

            self._timer_thread = threading.Thread(target=self._flush_loop, daemon=True)
            self._timer_thread.start()
            logging.info(f"S3Writer enabled | bucket={self.bucket} | flush_interval={self.flush_interval}s")
        else:
            logging.warning("S3Writer disabled (AWS credentials not configured). S3 uploads skipped.")

    def _get_partition_key(self, user_id: str) -> str:
        return f"user_id={user_id}"

    def _ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logging.info(f"Bucket exists: {self.bucket}")
        except ClientError:
            try:
                if BUCKET_REGION == "us-east-1":
                    self.s3_client.create_bucket(Bucket=self.bucket)
                else:
                    self.s3_client.create_bucket(
                        Bucket=self.bucket,
                        CreateBucketConfiguration={'LocationConstraint': BUCKET_REGION}
                    )
                logging.info(f"Created bucket: {self.bucket}")
            except ClientError as e:
                logging.error(f"Failed to create bucket: {e}")
                self.enabled = False

    def add(self, doc: dict[str, Any], user_id: str):
        if not self.enabled:
            return
        partition_key = self._get_partition_key(user_id)
        with self.lock:
            self.buffer[partition_key].append(doc)

    def _flush_loop(self):
        while self.running:
            time.sleep(self.flush_interval)
            self.flush()

    def flush(self):
        if not self.enabled:
            return

        client = self.s3_client
        if not client:
            return

        with self.lock:
            if not self.buffer:
                return

            buffers_to_flush = dict(self.buffer)
            self.buffer.clear()

        total_records = 0
        for partition_key, records in buffers_to_flush.items():
            if not records:
                continue

            timestamp = int(time.time())
            for i, record in enumerate(records):
                doc_id = record.get("id")
                s3_key = f"{partition_key}/{timestamp}_{doc_id}_{i}.jsonl"

                try:
                    content = json.dumps(record)
                    client.put_object(
                        Bucket=self.bucket,
                        Key=s3_key,
                        Body=content.encode("utf-8"),
                        ContentType="application/json"
                    )
                    total_records += 1
                except ClientError as e:
                    logging.error(f"S3 upload failed for {s3_key}: {e}")

            logging.info(f"Uploaded {len(records)} records to s3://{self.bucket}/{partition_key}/")

        logging.info(f"Flush complete | records={total_records} | partitions={len(buffers_to_flush)}")

    def close(self):
        self.running = False
        if self.enabled:
            self._timer_thread.join(timeout=5)
        self.flush()
        if self.enabled:
            logging.info("S3Writer closed")
