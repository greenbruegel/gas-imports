"""MongoDB helpers for the modern gas imports pipeline."""

from __future__ import annotations

import os
from functools import lru_cache

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.server_api import ServerApi


@lru_cache(maxsize=1)
def _get_client_and_db_name() -> tuple[MongoClient, str]:
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("GAS_IMPORTS_DB")

    missing = [name for name, value in {"MONGO_URI": mongo_uri, "GAS_IMPORTS_DB": db_name}.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required environment variable(s): {', '.join(missing)}")

    client = MongoClient(
        mongo_uri,
        server_api=ServerApi("1"),
        tlsCAFile=certifi.where(),
    )
    return client, db_name


def get_database() -> Database:
    """Return the configured gas imports MongoDB database."""
    client, db_name = _get_client_and_db_name()
    return client[db_name]
