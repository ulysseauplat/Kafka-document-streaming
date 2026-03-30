import sqlite3

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

    conn.commit()
    conn.close()


def insert_similarity(doc1, doc2, sim):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    doc1, doc2 = sorted((doc1, doc2))

    cursor.execute("""
        INSERT OR IGNORE INTO similarities (doc_id_1, doc_id_2, similarity)
        VALUES (?, ?, ?)
    """, (doc1, doc2, sim))

    conn.commit()
    conn.close()
