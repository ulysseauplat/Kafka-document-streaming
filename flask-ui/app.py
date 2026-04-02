import os
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, render_template, jsonify, request
import sqlite3
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ReadTimeoutError, ConnectTimeoutError
from datetime import datetime
import json
import time

app = Flask(__name__)

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/similarity.db")
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
        config=Config(connect_timeout=5, read_timeout=30, retries={'max_attempts': 2})
    )


def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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

        cur.execute("SELECT COUNT(*) as count FROM similarities")
        total_similarities = cur.fetchone()["count"]

        cur.execute("SELECT SUM(total_comments) as total FROM user_stats")
        result = cur.fetchone()
        total_comments = result["total"] if result["total"] else 0

        cur.execute("SELECT SUM(processed_count) as total FROM consumer_stats")
        result = cur.fetchone()
        total_processed = result["total"] if result["total"] else 0

        cur.execute("SELECT MIN(timestamp) as start_time FROM consumer_stats")
        result = cur.fetchone()
        start_time = result["start_time"] if result["start_time"] else None

        if start_time and total_processed > 0:
            elapsed = time.time() - start_time
            total_speed = total_processed / elapsed if elapsed > 0 else 0
        else:
            total_speed = 0

        conn.close()

        return jsonify({
            "total_comments": total_comments,
            "total_similarities": total_similarities,
            "total_processed": total_processed,
            "total_speed": round(total_speed, 2),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/spammers")
def get_spammers():
    try:
        threshold = float(request.args.get("threshold", 5)) / 100

        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                d1.user_id as user_id,
                u.total_comments,
                COUNT(*) as similar_pairs,
                CAST(COUNT(*) AS REAL) / NULLIF(u.total_comments * (u.total_comments - 1) / 2, 0) as similarity_rate
            FROM similarities s
            JOIN doc_users d1 ON s.doc_id_1 = d1.doc_id
            JOIN doc_users d2 ON s.doc_id_2 = d2.doc_id
            JOIN user_stats u ON d1.user_id = u.user_id
            WHERE d1.user_id = d2.user_id AND u.total_comments >= 2
            GROUP BY d1.user_id, u.total_comments
            HAVING CAST(COUNT(*) AS REAL) / NULLIF(u.total_comments * (u.total_comments - 1) / 2, 0) >= ?
            ORDER BY similarity_rate DESC
            LIMIT 100
        """, (threshold,))

        spammers = [dict(row) for row in cur.fetchall()]
        conn.close()

        return jsonify(spammers)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/consumer-stats")
def get_consumer_stats():
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT consumer_id, throughput, processed_count, timestamp FROM consumer_stats")
        rows = cur.fetchall()
        conn.close()

        stats = {}
        for row in rows:
            stats[row["consumer_id"]] = {
                "throughput": row["throughput"],
                "processed_count": row["processed_count"],
                "timestamp": row["timestamp"]
            }

        return jsonify(stats)
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

        cur.execute("""
            SELECT s.doc_id_1, s.doc_id_2, s.similarity,
                   d1.user_id as user_id_1,
                   d2.user_id as user_id_2
            FROM similarities s
            JOIN doc_users d1 ON s.doc_id_1 = d1.doc_id
            JOIN doc_users d2 ON s.doc_id_2 = d2.doc_id
            LIMIT ?
        """, (limit,))

        pairs = [dict(row) for row in cur.fetchall()]
        conn.close()

        user_ids = set()
        for p in pairs:
            user_ids.add(p["user_id_1"])
            user_ids.add(p["user_id_2"])

        doc_lookup = {}
        for user_id in user_ids:
            prefix = f"user_id={user_id}/"
            try:
                for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=prefix):
                    if "Contents" not in page:
                        continue
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if not key.endswith(".jsonl"):
                            continue
                        try:
                            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                            content = response["Body"].read().decode("utf-8")
                            doc = json.loads(content)
                            doc_id = doc.get("id")
                            if doc_id:
                                doc_lookup[doc_id] = {
                                    "text": doc.get("text"),
                                    "user_id": doc.get("user_id")
                                }
                        except (ClientError, json.JSONDecodeError):
                            continue
            except ClientError:
                continue

        results = []
        for p in pairs:
            doc1 = doc_lookup.get(p["doc_id_1"], {})
            doc2 = doc_lookup.get(p["doc_id_2"], {})
            results.append({
                "doc_id_1": p["doc_id_1"],
                "doc_id_2": p["doc_id_2"],
                "similarity": p["similarity"],
                "text_1": doc1.get("text"),
                "text_2": doc2.get("text"),
                "user_id_1": p["user_id_1"],
                "user_id_2": p["user_id_2"]
            })

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

        results = [dict(row) for row in cur.fetchall()]
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
                                        "timestamp": key.split("/")[-1].split("_")[0]
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

        return jsonify({
            "user_id": user_id,
            "similar_comments": similar_comments,
            "non_similar_comments": non_similar_comments,
            "total_found": len(similar_comments),
            "files_checked": files_checked
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
