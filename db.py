import sqlite3
from flask import g
import os

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_db_env = os.environ.get("DATABASE_PATH", "data.db").strip()
DATABASE = _db_env if os.path.isabs(_db_env) else os.path.join(_BACKEND_DIR, _db_env)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


def init_db():
    db = sqlite3.connect(DATABASE)
    db.row_factory = sqlite3.Row
    db.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            table_name TEXT NOT NULL UNIQUE,
            row_count INTEGER,
            col_count INTEGER,
            columns_json TEXT,
            sample_json TEXT,
            stats_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()
    db.close()