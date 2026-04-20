import psycopg2
from sqlalchemy import create_engine

DB_CONFIG = {
    "host": "localhost",
    "database": "t24GLreconDB",
    "user": "postgres",
    "password": "QWekujnr@11",
    "port": 5432
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_engine():
    return create_engine(
        "postgresql://postgres:QWekujnr%4011@localhost:5432/t24GLreconDB"
    )