
"""
NOVEAM Neon PostgreSQL connection check.

Read-only connection and version check.
Does not create, update, or delete tables.
"""

import logging
import os
import sys

import psycopg


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("noveam.neon")


def check_neon():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("DATABASE_URL is not configured")
        return False

    try:
        with psycopg.connect(
            database_url,
            connect_timeout=10,
            autocommit=True
        ) as conn:

            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT current_database(), "
                    "current_user, version()"
                )

                database, user, version = cursor.fetchone()

                logger.info(
                    "Neon connection successful"
                )

                logger.info(
                    "Database connection verified"
                )

                logger.info(
                    "PostgreSQL version: %s",
                    version.split()[1]
                )

        return True

    except psycopg.Error as exc:
        logger.error(
            "Neon connection failed: %s",
            type(exc).__name__
        )
        return False


if __name__ == "__main__":
    sys.exit(0 if check_neon() else 1)
