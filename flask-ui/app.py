import json
import os
from datetime import datetime

import boto3
import psycopg2
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

load_dotenv()

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "similarity_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "kafka-stream-project-ulysse")

s3_client = None
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 2}),
    )


def get_db():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    return conn


def dict_from_row(row):
    return dict(row._asdict()) if hasattr(row, '_asdict') else dict(row)


@app.route("/api/health")
def health_check():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/spammers")
def spammers_page():
    return render_template("spammers.html")


@app.route("/similar-comments")
def similar_comments_page():
    return render_template("similar-comments.html")


@app.route("/api/metrics")
def get_metrics():
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM similarities")
        total_similarities = cur.fetchone()[0] or 0

        cur.execute("SELECT SUM(total_comments) FROM user_stats")
        result = cur.fetchone()
        total_comments = result[0] if result and result[0] is not None else 0

        total_processed = total_comments

        cur.execute("SELECT SUM(throughput) FROM consumer_stats")
        result = cur.fetchone()
        total_throughput = result[0] if result and result[0] is not None else 0

        conn.close()

        print(f"DEBUG metrics: similarities={total_similarities}, comments={total_comments}, processed={total_processed}, throughput={total_throughput}")

        return jsonify(
            {
                "total_comments": total_comments,
                "total_similarities": total_similarities,
                "total_processed": total_processed,
                "total_throughput": total_throughput,
                "timestamp": datetime.now().isoformat(),
            }
        )
    except Exception as e:
        import traceback
        print(f"ERROR metrics: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/spammers")
def get_spammers():
    try:
        threshold = float(request.args.get("threshold", 5)) / 100
        min_comments = int(request.args.get("min_comments", 10))

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                d1.user_id as user_id,
                u.total_comments,
                COUNT(*) as similar_pairs,
                CAST(COUNT(*) AS REAL) / NULLIF(u.total_comments * (u.total_comments - 1) / 2, 0) as similarity_rate
            FROM similarities s
            JOIN doc_users d1 ON s.doc_id_1 = d1.doc_id
            JOIN doc_users d2 ON s.doc_id_2 = d2.doc_id
            JOIN user_stats u ON d1.user_id = u.user_id
            WHERE d1.user_id = d2.user_id AND u.total_comments >= %s
            GROUP BY d1.user_id, u.total_comments
            HAVING CAST(COUNT(*) AS REAL) / NULLIF(u.total_comments * (u.total_comments - 1) / 2, 0) >= %s
            ORDER BY similarity_rate DESC
            LIMIT 100
        """,
            (min_comments, threshold),
        )

        rows = cur.fetchall()
        spammers = []
        for row in rows:
            spammers.append({
                "user_id": row[0],
                "total_comments": row[1],
                "similar_pairs": row[2],
                "similarity_rate": row[3]
            })
        conn.close()

        return jsonify(spammers)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/consumer-stats")
def get_consumer_stats():
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            "SELECT consumer_id, throughput, processed_count, timestamp, avg_latency_ms, min_latency_ms, max_latency_ms FROM consumer_stats"
        )
        rows = cur.fetchall()
        conn.close()

        stats = {}
        for row in rows:
            stats[row[0]] = {
                "throughput": row[1],
                "processed_count": row[2],
                "timestamp": row[3],
                "avg_latency_ms": row[4],
                "min_latency_ms": row[5],
                "max_latency_ms": row[6],
            }

        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/system-latency")
def get_system_latency():
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            "SELECT total_latency_ms, latency_count, min_latency_ms, max_latency_ms FROM system_stats WHERE id = 1"
        )
        row = cur.fetchone()
        conn.close()

        if row and row[1] > 0:
            avg_latency = row[0] / row[1]
            return jsonify(
                {
                    "avg_latency_ms": round(avg_latency, 2),
                    "min_latency_ms": row[2],
                    "max_latency_ms": row[3],
                    "total_latency_ms": row[0],
                    "latency_count": row[1],
                }
            )
        return jsonify(
            {
                "avg_latency_ms": 0,
                "min_latency_ms": 0,
                "max_latency_ms": 0,
                "total_latency_ms": 0,
                "latency_count": 0,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/similar-comments")
def get_similar_comments():
    try:
        if not s3_client:
            return jsonify({"error": "S3 not configured"}), 500

        limit = int(request.args.get("limit", 50))

        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT s.doc_id_1, s.doc_id_2, s.similarity,
                   d1.user_id as user_id_1,
                   d2.user_id as user_id_2
            FROM similarities s
            JOIN doc_users d1 ON s.doc_id_1 = d1.doc_id
            JOIN doc_users d2 ON s.doc_id_2 = d2.doc_id
            LIMIT %s
        """,
            (limit,),
        )

        rows = cur.fetchall()
        pairs = []
        for row in rows:
            pairs.append({
                "doc_id_1": row[0],
                "doc_id_2": row[1],
                "similarity": row[2],
                "user_id_1": row[3],
                "user_id_2": row[4]
            })
        conn.close()

        user_ids = set()
        for p in pairs:
            user_ids.add(p["user_id_1"])
            user_ids.add(p["user_id_2"])

        doc_lookup = {}
        for user_id in user_ids:
            prefix = f"user_id={user_id}/"
            try:
                for page in s3_client.get_paginator("list_objects_v2").paginate(
                    Bucket=S3_BUCKET, Prefix=prefix
                ):
                    if "Contents" not in page:
                        continue
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if not key.endswith(".jsonl"):
                            continue
                        try:
                            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                            content = response["Body"].read().decode("utf-8").strip()
                            if not content:
                                continue
                            doc = json.loads(content)
                            doc_id = doc.get("id")
                            if doc_id:
                                doc_lookup[doc_id] = {
                                    "text": doc.get("text"),
                                    "user_id": doc.get("user_id"),
                                }
                        except (ClientError, json.JSONDecodeError, Exception):
                            continue
            except ClientError:
                continue

        results = []
        for p in pairs:
            doc1 = doc_lookup.get(p["doc_id_1"], {})
            doc2 = doc_lookup.get(p["doc_id_2"], {})
            results.append(
                {
                    "doc_id_1": p["doc_id_1"],
                    "doc_id_2": p["doc_id_2"],
                    "similarity": p["similarity"],
                    "text_1": doc1.get("text") if doc1 else None,
                    "text_2": doc2.get("text") if doc2 else None,
                    "user_id_1": p["user_id_1"],
                    "user_id_2": p["user_id_2"],
                }
            )

        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/top-users")
def get_top_users():
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT user_id, total_comments, similar_pairs, similarity_rate
            FROM user_stats
            WHERE total_comments > 10
            ORDER BY total_comments DESC
            LIMIT 10
        """)

        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append({
                "user_id": row[0],
                "total_comments": row[1],
                "similar_pairs": row[2],
                "similarity_rate": row[3]
            })
        conn.close()

        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/comments/<user_id>")
def get_comments(user_id):
    try:
        if not s3_client:
            return jsonify({"error": "S3 not configured"}), 500

        prefix = f"user_id={user_id}/"

        similar_comments = []
        files_checked = 0

        paginator = s3_client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    files_checked += 1
                    key = obj["Key"]
                    if not key.endswith(".jsonl"):
                        continue

                    try:
                        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                        content = response["Body"].read().decode("utf-8")

                        for line in content.strip().split("\n"):
                            if not line:
                                continue
                            try:
                                doc = json.loads(line)
                                if doc.get("user_id") == user_id:
                                    comment = {
                                        "id": doc.get("id"),
                                        "text": doc.get("text"),
                                        "timestamp": key.split("/")[-1].split("_")[0],
                                    }
                                    similar_comments.append(comment)
                            except json.JSONDecodeError:
                                continue

                    except (ClientError, ReadTimeoutError, ConnectTimeoutError):
                        continue

        except ClientError as e:
            return jsonify({"error": str(e)}), 500

        similar_comments = similar_comments[:10]
        non_similar_comments = similar_comments[10:15] if len(similar_comments) > 10 else []

        return jsonify(
            {
                "user_id": user_id,
                "similar_comments": similar_comments,
                "non_similar_comments": non_similar_comments,
                "total_found": len(similar_comments),
                "files_checked": files_checked,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
