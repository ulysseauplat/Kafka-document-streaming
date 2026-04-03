import os

import pytest

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "similarity_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


@pytest.fixture
def sample_comment():
    return "This is a test comment for similarity detection"


@pytest.fixture
def sample_comments():
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
