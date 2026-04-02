import sqlite3
import time

DB_PATH = "/data/similarity.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS similarities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id_1 INTEGER,
            doc_id_2 INTEGER,
            similarity REAL,
            UNIQUE(doc_id_1, doc_id_2)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_ids ON similarities(doc_id_1, doc_id_2)")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id TEXT PRIMARY KEY,
            total_comments INTEGER DEFAULT 0,
            similar_pairs INTEGER DEFAULT 0,
            similarity_rate REAL DEFAULT 0.0
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON user_stats(user_id)")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS doc_users (
            doc_id INTEGER PRIMARY KEY,
            user_id TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_user ON doc_users(user_id)")

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

    try:
        cursor.execute("ALTER TABLE consumer_stats ADD COLUMN avg_latency_ms REAL DEFAULT 0")
        cursor.execute("ALTER TABLE consumer_stats ADD COLUMN min_latency_ms REAL DEFAULT 0")
        cursor.execute("ALTER TABLE consumer_stats ADD COLUMN max_latency_ms REAL DEFAULT 0")
        cursor.execute("ALTER TABLE consumer_stats ADD COLUMN total_latency_ms REAL DEFAULT 0")
        cursor.execute("ALTER TABLE consumer_stats ADD COLUMN latency_count INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute(
            """
            INSERT INTO system_stats (id, last_updated)
            VALUES (1, :ts)
            ON CONFLICT(id) DO NOTHING
        """,
            {"ts": int(time.time())},
        )
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


def insert_similarity(doc1, doc2, sim):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    doc1, doc2 = sorted((doc1, doc2))

    cursor.execute(
        """
        INSERT OR IGNORE INTO similarities (doc_id_1, doc_id_2, similarity)
        VALUES (?, ?, ?)
    """,
        (doc1, doc2, sim),
    )

    conn.commit()
    conn.close()


def recalculate_user_stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM user_stats")

    cursor.execute("""
        INSERT INTO user_stats (user_id, total_comments, similar_pairs, similarity_rate)
        SELECT user_id, COUNT(*) as total_comments, 0, 0.0
        FROM doc_users
        GROUP BY user_id
    """)

    conn.commit()
    conn.close()


def update_user_stats_comment(user_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO user_stats (user_id, total_comments)
        VALUES (?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            total_comments = total_comments + 1
    """,
        (user_id,),
    )
    conn.commit()
    conn.close()


def sync_user_stats_from_doc_users():
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()


def track_doc_user(doc_id, user_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT OR IGNORE INTO doc_users (doc_id, user_id)
        VALUES (?, ?)
    """,
        (doc_id, user_id),
    )

    conn.commit()
    conn.close()


def get_user_for_doc(doc_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = ?", (doc_id,))
    result = cursor.fetchone()
    conn.close()

    return result[0] if result else None


def update_user_stats_for_similarity(doc1, doc2):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = ?", (doc1,))
    result1 = cursor.fetchone()

    cursor.execute("SELECT user_id FROM doc_users WHERE doc_id = ?", (doc2,))
    result2 = cursor.fetchone()

    conn.close()

    if result1 and result2 and result1[0] == result2[0]:
        user_id = result1[0]

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE user_stats
            SET similar_pairs = similar_pairs + 1,
                similarity_rate = CAST(similar_pairs + 1 AS REAL) / NULLIF(total_comments, 0)
            WHERE user_id = ?
        """,
            (user_id,),
        )

        conn.commit()
        conn.close()


def recalculate_all_similarity_rates():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE user_stats
        SET similarity_rate = CAST(similar_pairs AS REAL) / NULLIF(total_comments, 0)
    """)

    conn.commit()
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
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO consumer_stats (consumer_id, timestamp, throughput, processed_count, avg_latency_ms, min_latency_ms, max_latency_ms, total_latency_ms, latency_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    conn.close()


def update_system_latency(
    total_latency_ms: float, latency_count: int, min_latency_ms: float, max_latency_ms: float
):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE system_stats
        SET total_latency_ms = total_latency_ms + ?,
            latency_count = latency_count + ?,
            min_latency_ms = CASE
                WHEN latency_count = 0 THEN ?
                ELSE MIN(min_latency_ms, ?)
            END,
            max_latency_ms = CASE
                WHEN latency_count = 0 THEN ?
                ELSE MAX(max_latency_ms, ?)
            END,
            last_updated = ?
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
    conn.close()


def get_consumer_stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT consumer_id, throughput, processed_count, timestamp, avg_latency_ms, min_latency_ms, max_latency_ms FROM consumer_stats"
    )
    results = {}
    for row in cursor.fetchall():
        results[row["consumer_id"]] = {
            "throughput": row["throughput"],
            "processed_count": row["processed_count"],
            "timestamp": row["timestamp"],
            "avg_latency_ms": row["avg_latency_ms"],
            "min_latency_ms": row["min_latency_ms"],
            "max_latency_ms": row["max_latency_ms"],
        }
    conn.close()

    return results


def get_system_latency_stats():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT total_latency_ms, latency_count, min_latency_ms, max_latency_ms FROM system_stats WHERE id = 1"
    )
    row = cursor.fetchone()
    conn.close()

    if row and row["latency_count"] > 0:
        return {
            "total_latency_ms": row["total_latency_ms"],
            "latency_count": row["latency_count"],
            "min_latency_ms": row["min_latency_ms"],
            "max_latency_ms": row["max_latency_ms"],
            "avg_latency_ms": row["total_latency_ms"] / row["latency_count"],
        }
    return {
        "total_latency_ms": 0,
        "latency_count": 0,
        "min_latency_ms": 0,
        "max_latency_ms": 0,
        "avg_latency_ms": 0,
    }
