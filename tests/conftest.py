import os
import sqlite3
import sys
import tempfile

import pytest

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
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def sample_comment():
    """Sample comment for testing."""
    return "This is a test comment for similarity detection"


@pytest.fixture
def sample_comments():
    """Multiple sample comments with varying similarity."""
    return {
        "identical": [
            "Buy cheap watches online now click here for deals",
            "Buy cheap watches online now click here for deals"
        ],
        "similar": [
            "Buy cheap watches online now click here for deals",
            "Buy cheap watches online click now for best deals"
        ],
        "different": [
            "The weather today is beautiful and sunny",
            "I love eating pizza with friends on weekends"
        ]
    }
