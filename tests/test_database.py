import os

import psycopg2
import pytest

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "similarity_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


@pytest.fixture(scope="session", autouse=True)
def init_database():
    from database.database import init_db
    init_db()


@pytest.fixture
def db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def cleanup_test_data(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("DELETE FROM similarities WHERE doc_id_1 >= 999999 OR doc_id_2 >= 999999")
    cursor.execute("DELETE FROM user_stats WHERE user_id LIKE 'test_%'")
    cursor.execute("DELETE FROM doc_users WHERE doc_id >= 999999")
    yield
    cursor.execute("DELETE FROM similarities WHERE doc_id_1 >= 999999 OR doc_id_2 >= 999999")
    cursor.execute("DELETE FROM user_stats WHERE user_id LIKE 'test_%'")
    cursor.execute("DELETE FROM doc_users WHERE doc_id >= 999999")
    cursor.close()


class TestInsertSimilarity:
    def test_insert_similarity(self, db_connection):
        from database.database import insert_similarity, track_doc_user

        doc_id_1 = 999999
        doc_id_2 = 999998
        user_id = "test_user_1"

        track_doc_user(doc_id_1, user_id)
        track_doc_user(doc_id_2, user_id)

        insert_similarity(doc_id_1, doc_id_2, 0.85, 1)

        cursor = db_connection.cursor()
        cursor.execute("SELECT doc_id_1, doc_id_2, similarity FROM similarities WHERE doc_id_1 = %s", (doc_id_1,))
        row = cursor.fetchone()
        assert row is not None, "Similarity was not inserted"
        assert row[0] == doc_id_1
        assert row[1] == doc_id_2
        assert row[2] == 0.85

    def test_insert_similarity_order_independent(self, db_connection):
        from database.database import insert_similarity, track_doc_user

        doc_id_1 = 999999
        doc_id_2 = 999998
        user_id = "test_user_2"

        track_doc_user(doc_id_1, user_id)
        track_doc_user(doc_id_2, user_id)

        insert_similarity(doc_id_2, doc_id_1, 0.75, 1)

        cursor = db_connection.cursor()
        cursor.execute("SELECT doc_id_1, doc_id_2 FROM similarities WHERE doc_id_1 = %s", (doc_id_1,))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == doc_id_1, "Doc IDs should be sorted (smaller first)"
        assert row[1] == doc_id_2

    def test_insert_similarity_duplicate_ignored(self, db_connection):
        from database.database import insert_similarity, track_doc_user

        doc_id_1 = 999999
        doc_id_2 = 999998
        user_id = "test_user_3"

        track_doc_user(doc_id_1, user_id)
        track_doc_user(doc_id_2, user_id)

        insert_similarity(doc_id_1, doc_id_2, 0.85, 1)
        insert_similarity(doc_id_1, doc_id_2, 0.90, 1)

        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM similarities WHERE doc_id_1 = %s AND doc_id_2 = %s", (doc_id_1, doc_id_2))
        count = cursor.fetchone()[0]
        assert count == 1, "Duplicate pair should not be inserted"
