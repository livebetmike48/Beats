import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.getenv("DB_PATH", "twitter_bot.db")

# Set once, the first time keywords are seeded from keywords.DEFAULT_GROUPS.
# After that the DB is the source of truth and the file is never re-read,
# so /addword and /removeword survive redeploys.
_SEED_FLAG = "keywords_seeded"


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
        c.execute("""
            CREATE TABLE IF NOT EXISTS keywords (
                group_key TEXT NOT NULL,
                phrase TEXT NOT NULL,
                added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_key, phrase)
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


# ---------------------------------------------------------------------------
# Keywords
# ---------------------------------------------------------------------------
def seed_keywords(default_groups: dict) -> int:
    """One-time load of keywords.DEFAULT_GROUPS into the DB. Runs only if the
    seed flag is unset, so deleting a keyword in Discord stays deleted across
    restarts instead of being resurrected by the file. Returns rows inserted
    (0 on every boot after the first)."""
    if get_config(_SEED_FLAG):
        return 0
    inserted = 0
    with _conn() as c:
        for group_key, grp in (default_groups or {}).items():
            for phrase in grp.get("keywords", []):
                p = " ".join(str(phrase).strip().lower().split())
                if not p:
                    continue
                cur = c.execute(
                    "INSERT OR IGNORE INTO keywords (group_key, phrase) VALUES (?, ?)",
                    (group_key, p),
                )
                inserted += cur.rowcount
    set_config(_SEED_FLAG, "1")
    return inserted


def get_keywords() -> dict:
    """{group_key: [phrase, ...]} sorted, straight from the DB."""
    out: dict[str, list[str]] = {}
    with _conn() as c:
        rows = c.execute(
            "SELECT group_key, phrase FROM keywords ORDER BY group_key, phrase"
        ).fetchall()
    for r in rows:
        out.setdefault(r["group_key"], []).append(r["phrase"])
    return out


def add_keyword(group_key: str, phrase: str) -> bool:
    """True if inserted, False if that group already had it."""
    with _conn() as c:
        cur = c.execute(
            "INSERT OR IGNORE INTO keywords (group_key, phrase) VALUES (?, ?)",
            (group_key, phrase),
        )
        return cur.rowcount > 0


def remove_keyword(group_key: str, phrase: str) -> bool:
    """True if a row was deleted, False if it wasn't there."""
    with _conn() as c:
        cur = c.execute(
            "DELETE FROM keywords WHERE group_key = ? AND phrase = ?",
            (group_key, phrase),
        )
        return cur.rowcount > 0
