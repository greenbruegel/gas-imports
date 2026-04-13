"""MongoDB client setup for this project.

Reads connection settings from environment/.env:
- MONGO_URI
- MONGO_DB_NAME
"""

from __future__ import annotations

import os

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.server_api import ServerApi

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

if not all([MONGO_URI, MONGO_DB_NAME]):
    raise RuntimeError("Missing required MongoDB environment variables: MONGO_URI, MONGO_DB_NAME")

mongo_client = MongoClient(
    MONGO_URI,
    server_api=ServerApi("1"),
    tlsCAFile=certifi.where(),
)
db: Database = mongo_client[MONGO_DB_NAME]


def get_database() -> Database:
    """Return the configured MongoDB database handle."""
    return db


def test_mongo_connection() -> None:
    """Ping MongoDB to validate client connectivity."""
    try:
        mongo_client.admin.command("ping")
    except Exception as exc:
        raise RuntimeError(f"MongoDB connection error: {exc}") from exc
