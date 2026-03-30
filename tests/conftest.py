import os
import sys
import pytest
import tempfile
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = tempfile.mktemp(suffix=".db")


@pytest.fixture(autouse=True)
def reset_db_path(monkeypatch):
    import database.database
    monkeypatch.setattr(database.database, "DB_PATH", DB_PATH)
    yield
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


@pytest.fixture
def db_connection():
    conn = sqlite3.connect(DB_PATH)
    yield conn
    conn.close()
