
"""
NOVEAM SQLite Database Backup

Creates a timestamped backup of an existing
SQLite database using the SQLite backup API.

Does not delete or modify source records.
"""

from pathlib import Path
from datetime import datetime, timezone
import logging
import os
import sqlite3
import time
import uuid


# ========================================
# 1. LOGGING CONFIGURATION
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    )
)

logger = logging.getLogger(
    "noveam.database.backup"
)


# ========================================
# 2. BACKUP FUNCTION
# ========================================

def backup_database():

    started = time.perf_counter()

    logger.info(
        "NOVEAM SQLite backup started"
    )

    here = Path(__file__).resolve().parent

    # Match the environment variable used by
    # the newer NOVEAM main.py.
    db_path = Path(
        os.environ.get(
            "NOVEAM_DB_PATH",
            str(here / "noveam.db")
        )
    ).resolve()

    # Never create an empty source database.
    if not db_path.is_file():

        logger.error(
            "Source SQLite database not found"
        )

        raise FileNotFoundError(
            "The configured source database "
            "does not exist. Check NOVEAM_DB_PATH."
        )

    backup_dir = here / "backups"

    backup_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    stamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S_%f")

    unique_id = uuid.uuid4().hex[:8]

    backup_path = (
        backup_dir
        / f"noveam_{stamp}_{unique_id}.db"
    )

    logger.info(
        "Source database found; "
        "creating SQLite snapshot"
    )

    try:

        # mode=ro prevents SQLite from creating
        # or writing to the source database.
        source_uri = (
            db_path.as_uri() + "?mode=ro"
        )

        with sqlite3.connect(
            source_uri,
            uri=True
        ) as source:

            with sqlite3.connect(
                backup_path
            ) as destination:

                source.backup(destination)

        logger.info(
            "SQLite backup copy completed"
        )

        # ====================================
        # 3. VERIFY BACKUP INTEGRITY
        # ====================================

        backup_uri = (
            backup_path.as_uri() + "?mode=ro"
        )

        with sqlite3.connect(
            backup_uri,
            uri=True
        ) as verify_conn:

            integrity = [
                row[0]
                for row in verify_conn.execute(
                    "PRAGMA quick_check"
                )
            ]

            violations = list(
                verify_conn.execute(
                    "PRAGMA foreign_key_check"
                )
            )

        if integrity != ["ok"]:

            logger.error(
                "Backup verification failed: "
                "SQLite structural integrity"
            )

            raise RuntimeError(
                "Backup failed SQLite quick_check"
            )

        if violations:

            logger.warning(
                "Backup contains %s foreign-key "
                "violations",
                len(violations)
            )

        else:

            logger.info(
                "Backup foreign-key check passed"
            )

        duration_ms = (
            time.perf_counter() - started
        ) * 1000

        backup_size = backup_path.stat().st_size

        logger.info(
            "NOVEAM backup completed successfully; "
            "size_bytes=%s duration_ms=%.2f",
            backup_size,
            duration_ms
        )

        print(
            "Backup saved successfully:",
            backup_path
        )

        return backup_path

    except Exception:

        logger.error(
            "NOVEAM backup failed; "
            "backup must not be treated as verified"
        )

        raise


# ========================================
# 4. RUN BACKUP
# ========================================

if __name__ == "__main__":

    backup_database()
