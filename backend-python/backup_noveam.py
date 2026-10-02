from pathlib import Path
import sqlite3, datetime, os

here=Path(__file__).resolve().parent
db=Path(os.environ.get("NOVEAGAMHEAM_DB_PATH", here/"noveagamheam.db"))
out=here/"backups"
out.mkdir(exist_ok=True)
stamp=datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
dest=out/f"noveam_{stamp}.db"
with sqlite3.connect(db) as src, sqlite3.connect(dest) as dst:
    src.backup(dst)
print(dest)
