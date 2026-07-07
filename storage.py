import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.getenv("DB_PATH", "twitter_bot.db")


@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS posted_tweets (
                tweet_id TEXT PRIMARY KEY,
                posted_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)


def already_posted(tweet_id: str) -> bool:
    with _conn() as c:
        row = c.execute("SELECT 1 FROM posted_tweets WHERE tweet_id = ?", (tweet_id,)).fetchone()
        return row is not None


def mark_posted(tweet_id: str):
    with _conn() as c:
        c.execute("INSERT OR IGNORE INTO posted_tweets (tweet_id) VALUES (?)", (tweet_id,))


def set_config(key: str, value: str):
    with _conn() as c:
        c.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, value))


def get_config(key: str) -> str | None:
    with _conn() as c:
        row = c.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None
