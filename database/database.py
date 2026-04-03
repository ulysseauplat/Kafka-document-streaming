import os
import time

import psycopg2

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "similarity_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS similarities (
                id TEXT PRIMARY KEY,
                doc_id_1 INTEGER NOT NULL,
                doc_id_2 INTEGER NOT NULL,
                similarity REAL NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
    except psycopg2.errors.UniqueViolation:
        pass
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_ids ON similarities(doc_id_1, doc_id_2)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_id_2 ON similarities(doc_id_2)")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id TEXT PRIMARY KEY,
                total_comments INTEGER DEFAULT 0,
                similar_pairs INTEGER DEFAULT 0,
                similarity_rate REAL DEFAULT 0.0
            )
        """)
    except psycopg2.errors.UniqueViolation:
        pass
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON user_stats(user_id)")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS doc_users (
                doc_id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL
            )
        """)
    except psycopg2.errors.UniqueViolation:
        pass
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_user ON doc_users(user_id)")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consumer_stats (
                consumer_id INTEGER PRIMARY KEY,
                timestamp INTEGER,
                throughput REAL,
                processed_count INTEGER,
                avg_latency_ms REAL DEFAULT 0,
                min_latency_ms REAL DEFAULT 0,
                max_latency_ms REAL DEFAULT 0,
                total_latency_ms REAL DEFAULT 0,
                latency_count INTEGER DEFAULT 0
            )
        """)
    except psycopg2.errors.UniqueViolation:
        pass

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_latency_ms REAL DEFAULT 0,
                latency_count INTEGER DEFAULT 0,
                min_latency_ms REAL DEFAULT 0,
                max_latency_ms REAL DEFAULT 0,
                last_updated INTEGER
            )
        """)
    except psycopg2.errors.UniqueViolation:
        pass

    try:
        cursor.execute(
            """
            INSERT INTO system_stats (id, last_updated)
            VALUES (1, %s)
            ON CONFLICT(id) DO NOTHING
        """,
            (int(time.time()),),
        )
    except psycopg2.errors.UniqueViolation:
        pass

    conn.commit()
    cursor.close()
    conn.close()


def insert_similarity(doc1, doc2, sim, consumer_id):
    conn = get_connection()
    cursor = conn.cursor()

    doc1, doc2 = sorted((doc1, doc2))

    unique_id = f"{consumer_id}_{doc1}_{doc2}_{int(time.time() * 1000000)}"

    cursor.execute(
        """
        INSERT INTO similarities (id, doc_id_1, doc_id_2, similarity)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """,
        (unique_id, doc1, doc2, sim),
    )

    conn.commit()
    cursor.close()
    conn.close()


def recalculate_user_stats():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM user_stats")

    cursor.execute("""
        INSERT INTO user_stats (user_id, total_comments, similar_pairs, similarity_rate)
        SELECT user_id, COUNT(*) as total_comments, 0, 0.0
        FROM doc_users
        GROUP BY user_id
    """)

    conn.commit()
    cursor.close()
    conn.close()


def update_user_stats_comment(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO user_stats (user_id, total_comments)
        VALUES (%s, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            total_comments = user_stats.total_comments + 1
    """,
        (user_id,),
    )
    conn.commit()
    cursor.close()
    conn.close()


def sync_user_stats_from_doc_users():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO user_stats (user_id, total_comments)
        SELECT user_id, COUNT(*) as cnt
        FROM doc_users
        GROUP BY user_id
        ON CONFLICT(user_id) DO UPDATE SET
            total_comments = excluded.total_comments
    """)
    conn.commit()
    cursor.close()
    conn.close()


def track_doc_user(doc_id, user_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO doc_users (doc_id, user_id)
        VALUES (%s, %s)
        ON CONFLICT (doc_id) DO NOTHING
    """,
        (doc_id, user_id),
    )

    conn.commit()
    cursor.close()
    conn.close()


def get_user_for_doc(doc_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = %s", (doc_id,))
    result = cursor.fetchone()
    conn.close()

    return result[0] if result else None


def update_user_stats_for_similarity(doc1, doc2):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = %s", (doc1,))
    result1 = cursor.fetchone()

    cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = %s", (doc2,))
    result2 = cursor.fetchone()

    if result1 and result2 and result1[0] == result2[0]:
        user_id = result1[0]

        cursor.execute(
            "SELECT total_comments, similar_pairs FROM user_stats WHERE user_id = %s",
            (user_id,)
        )
        row = cursor.fetchone()
        if row:
            total_comments = row[0]
            similar_pairs = row[1] + 1
            similarity_rate = similar_pairs / (total_comments * (total_comments - 1) / 2) if total_comments > 1 else 0

            cursor.execute(
                """
                UPDATE user_stats
                SET similar_pairs = %s, similarity_rate = %s
                WHERE user_id = %s
            """,
                (similar_pairs, similarity_rate, user_id),
            )

            conn.commit()

    conn.close()


def recalculate_all_similarity_rates():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE user_stats
        SET similarity_rate = CAST(similar_pairs AS REAL) / NULLIF(total_comments * (total_comments - 1) / 2, 0)
    """)

    conn.commit()
    cursor.close()
    conn.close()


def update_consumer_stats(
    consumer_id: int,
    throughput: float,
    processed_count: int,
    avg_latency_ms: float = 0,
    min_latency_ms: float = 0,
    max_latency_ms: float = 0,
    total_latency_ms: float = 0,
    latency_count: int = 0,
):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO consumer_stats (consumer_id, timestamp, throughput, processed_count, avg_latency_ms, min_latency_ms, max_latency_ms, total_latency_ms, latency_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(consumer_id) DO UPDATE SET
            timestamp = excluded.timestamp,
            throughput = excluded.throughput,
            processed_count = excluded.processed_count,
            avg_latency_ms = excluded.avg_latency_ms,
            min_latency_ms = excluded.min_latency_ms,
            max_latency_ms = excluded.max_latency_ms,
            total_latency_ms = excluded.total_latency_ms,
            latency_count = excluded.latency_count
    """,
        (
            consumer_id,
            int(time.time()),
            throughput,
            processed_count,
            avg_latency_ms,
            min_latency_ms,
            max_latency_ms,
            total_latency_ms,
            latency_count,
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()


def update_system_latency(
    total_latency_ms: float, latency_count: int, min_latency_ms: float, max_latency_ms: float
):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE system_stats
        SET total_latency_ms = system_stats.total_latency_ms + %s,
            latency_count = system_stats.latency_count + %s,
            min_latency_ms = CASE
                WHEN system_stats.latency_count = 0 THEN %s
                ELSE LEAST(system_stats.min_latency_ms, %s)
            END,
            max_latency_ms = CASE
                WHEN system_stats.latency_count = 0 THEN %s
                ELSE GREATEST(system_stats.max_latency_ms, %s)
            END,
            last_updated = %s
        WHERE id = 1
    """,
        (
            total_latency_ms,
            latency_count,
            min_latency_ms,
            min_latency_ms,
            max_latency_ms,
            max_latency_ms,
            int(time.time()),
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()


def get_consumer_stats():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT consumer_id, throughput, processed_count, timestamp, avg_latency_ms, min_latency_ms, max_latency_ms FROM consumer_stats"
    )
    results = {}
    for row in cursor.fetchall():
        results[row[0]] = {
            "throughput": row[1],
            "processed_count": row[2],
            "timestamp": row[3],
            "avg_latency_ms": row[4],
            "min_latency_ms": row[5],
            "max_latency_ms": row[6],
        }
    conn.close()

    return results


def get_system_latency_stats():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT total_latency_ms, latency_count, min_latency_ms, max_latency_ms FROM system_stats WHERE id = 1"
    )
    row = cursor.fetchone()
    conn.close()

    if row and row[1] > 0:
        return {
            "total_latency_ms": row[0],
            "latency_count": row[1],
            "min_latency_ms": row[2],
            "max_latency_ms": row[3],
            "avg_latency_ms": row[0] / row[1],
        }
    return {
        "total_latency_ms": 0,
        "latency_count": 0,
        "min_latency_ms": 0,
        "max_latency_ms": 0,
        "avg_latency_ms": 0,
    }


def batch_track_doc_users(pairs: list[tuple]):
    if not pairs:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO doc_users (doc_id, user_id) VALUES (%s, %s) ON CONFLICT (doc_id) DO NOTHING",
        pairs,
    )
    conn.commit()
    cursor.close()
    conn.close()


def batch_insert_similarities(triples: list[tuple], consumer_id: int):
    if not triples:
        return
    conn = get_connection()
    cursor = conn.cursor()
    values = []
    for doc1, doc2, sim in triples:
        doc1, doc2 = sorted((doc1, doc2))
        unique_id = f"{consumer_id}_{doc1}_{doc2}_{int(time.time() * 1000000)}"
        values.append((unique_id, doc1, doc2, sim))
    cursor.executemany(
        "INSERT INTO similarities (id, doc_id_1, doc_id_2, similarity) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
        values,
    )
    conn.commit()
    cursor.close()
    conn.close()


def batch_update_user_stats_comments(user_ids: list[str]):
    if not user_ids:
        return
    conn = get_connection()
    cursor = conn.cursor()
    from collections import Counter
    counts = Counter(user_ids)
    for user_id, count in counts.items():
        cursor.execute(
            """
            INSERT INTO user_stats (user_id, total_comments)
            VALUES (%s, %s)
            ON CONFLICT(user_id) DO UPDATE SET
                total_comments = user_stats.total_comments + %s
        """,
            (user_id, count, count),
        )
    conn.commit()
    cursor.close()
    conn.close()


def batch_update_user_stats_similarities(doc_pairs: list[tuple], consumer_id: int):
    if not doc_pairs:
        return
    conn = get_connection()
    cursor = conn.cursor()

    user_pair_counts: dict[str, int] = {}
    for doc1, doc2 in doc_pairs:
        cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = %s", (doc1,))
        result1 = cursor.fetchone()
        cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = %s", (doc2,))
        result2 = cursor.fetchone()

        if result1 and result2 and result1[0] == result2[0]:
            user_id = result1[0]
            user_pair_counts[user_id] = user_pair_counts.get(user_id, 0) + 1

    for user_id, count in user_pair_counts.items():
        cursor.execute(
            """
            UPDATE user_stats
            SET similar_pairs = similar_pairs + %s,
                similarity_rate = CAST(similar_pairs + %s AS REAL) / NULLIF(total_comments * (total_comments - 1) / 2, 0)
            WHERE user_id = %s
        """,
            (count, count, user_id),
        )

    conn.commit()
    cursor.close()
    conn.close()
