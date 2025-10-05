# db.py
from dotenv import load_dotenv
load_dotenv()  # must run before importing db.py or using os.environ

import os
import psycopg2
import ssl



def get_connection():
    """
    Returns a psycopg2 connection with SSL.
    Reads env vars: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT (optional)
    """
    host = os.environ.get("DB_HOST")
    dbname = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    port = os.environ.get("DB_PORT", 5432)

    if not all([host, dbname, user, password]):
        raise RuntimeError("Database environment variables missing (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)")

    # psycopg2 handles sslmode natively:
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        sslmode="require"  # <--- this is the key to avoid SSL errors
    )
    return conn
