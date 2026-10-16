# check_neon.py
import logging
import sys

import psycopg

from neon_db import test_neon_connection


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("noveam.neon")


def check_neon():
    try:
        result = test_neon_connection()

        logger.info("Neon connection successful")
        logger.info("Database: %s", result[0])

        return True

    except (psycopg.Error, OSError, KeyError) as exc:
        logger.error(
            "Neon connection failed: %s",
            type(exc).__name__
        )
        return False


if __name__ == "__main__":
    sys.exit(0 if check_neon() else 1)
