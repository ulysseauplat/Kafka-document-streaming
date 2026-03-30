import pytest
from database.database import init_db, insert_similarity


class TestInitDb:
    def test_init_db_creates_table(self, db_connection):
        init_db()
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='similarities'
        """)
        result = cursor.fetchone()
        assert result is not None, "Table 'similarities' was not created"

    def test_init_db_creates_index(self, db_connection):
        init_db()
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND name='idx_doc_ids'
        """)
        result = cursor.fetchone()
        assert result is not None, "Index 'idx_doc_ids' was not created"

    def test_init_db_idempotent(self, db_connection):
        init_db()
        init_db()
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        count = cursor.fetchone()[0]
        assert count >= 1


class TestInsertSimilarity:
    def test_insert_similarity(self, db_connection):
        init_db()
        insert_similarity(1, 2, 0.85)
        cursor = db_connection.cursor()
        cursor.execute("SELECT doc_id_1, doc_id_2, similarity FROM similarities")
        row = cursor.fetchone()
        assert row == (1, 2, 0.85)

    def test_insert_similarity_order_independent(self, db_connection):
        init_db()
        insert_similarity(2, 1, 0.75)
        cursor = db_connection.cursor()
        cursor.execute("SELECT doc_id_1, doc_id_2 FROM similarities")
        row = cursor.fetchone()
        assert row == (1, 2), "Doc IDs should be sorted (smaller first)"

    def test_insert_similarity_duplicate_ignored(self, db_connection):
        init_db()
        insert_similarity(1, 2, 0.85)
        insert_similarity(1, 2, 0.90)
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM similarities")
        count = cursor.fetchone()[0]
        assert count == 1, "Duplicate pair should not be inserted"

    def test_insert_similarity_reverse_duplicate_ignored(self, db_connection):
        init_db()
        insert_similarity(1, 2, 0.85)
        insert_similarity(2, 1, 0.90)
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM similarities")
        count = cursor.fetchone()[0]
        assert count == 1, "Reversed pair should not create duplicate"

    def test_insert_multiple_similarities(self, db_connection):
        init_db()
        insert_similarity(1, 2, 0.85)
        insert_similarity(3, 4, 0.75)
        insert_similarity(5, 6, 0.95)
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM similarities")
        count = cursor.fetchone()[0]
        assert count == 3
