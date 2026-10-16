"""Read-only integrity check; never creates or modifies a database."""

import argparse
import collections
from contextlib import closing
import pathlib
import sqlite3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=pathlib.Path)
    path = parser.parse_args().database.resolve()
    if not path.is_file():
        parser.error("Database does not exist")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        integrity = [row[0] for row in conn.execute("PRAGMA quick_check")]
        violations = list(conn.execute("PRAGMA foreign_key_check"))
    print("SQLite quick_check:", "ok" if integrity == ["ok"] else "issues found")
    print("Foreign-key violations:", len(violations))
    for table, count in sorted(
        collections.Counter(row[0] for row in violations).items()
    ):
        print(f"  {table}: {count}")
    return 0 if integrity == ["ok"] and not violations else 1


if __name__ == "__main__":
    raise SystemExit(main())
