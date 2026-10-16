# neon_db.py
import os
import psycopg

def test_neon_connection():
    url = os.environ["DATABASE_URL"]

    with psycopg.connect(
        url,
        connect_timeout=10
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database(), 1")
            return cursor.fetchone()
