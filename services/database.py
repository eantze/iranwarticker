"""SQLite database for storing market data."""

import json
import logging
import os
import sqlite3
import threading

logger = logging.getLogger(__name__)

# Persistent path: ./data/market_data.db relative to project root
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_PROJECT_ROOT, "data", "market_data.db")
_local = threading.local()


def _get_conn():
    """Get a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, timeout=10)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
    return _local.conn


def init_db():
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS quotes (
            symbol TEXT PRIMARY KEY,
            price REAL,
            change REAL,
            change_pct TEXT,
            updated_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS history (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            volume REAL,
            PRIMARY KEY (symbol, date)
        );

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS casualties (
            date TEXT NOT NULL,
            category TEXT NOT NULL,
            value INTEGER NOT NULL,
            source TEXT DEFAULT 'gemini_estimate',
            fetched_at INTEGER,
            PRIMARY KEY (date, category)
        );

        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            title TEXT,
            source_group TEXT DEFAULT 'general',
            fetched_at INTEGER,
            UNIQUE(url, source_group)
        );

        CREATE TABLE IF NOT EXISTS aaa_gas_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            current_regular REAL NOT NULL,
            month_ago_regular REAL NOT NULL,
            fetched_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS resolved_sources (
            source_id INTEGER PRIMARY KEY REFERENCES sources(id),
            resolved_url TEXT,
            resolved_title TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            resolved_at INTEGER,
            created_at INTEGER
        );
    """)
    conn.commit()
    logger.info("Database initialized at %s", DB_PATH)


def save_quote(symbol, price, change, change_pct):
    """Save or update a quote."""
    import time
    conn = _get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO quotes (symbol, price, change, change_pct, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        (symbol, price, change, change_pct, int(time.time())),
    )
    conn.commit()


def save_history(symbol, records):
    """Save historical price records. Each record is a dict with at least 'date' and 'close'."""
    conn = _get_conn()
    for rec in records:
        conn.execute(
            """INSERT OR REPLACE INTO history (symbol, date, close, open, high, low, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                symbol,
                rec["date"],
                rec["close"],
                rec.get("open"),
                rec.get("high"),
                rec.get("low"),
                rec.get("volume"),
            ),
        )
    conn.commit()


def get_quote(symbol):
    """Get the latest quote for a symbol."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT price, change, change_pct, updated_at FROM quotes WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    if row:
        return {
            "price": row["price"],
            "change": row["change"],
            "change_pct": row["change_pct"],
            "updated_at": row["updated_at"],
        }
    return None


def get_history(symbol, limit=30):
    """Get historical prices for a symbol, ordered by date ascending."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT date, close FROM history WHERE symbol = ? ORDER BY date ASC LIMIT ?",
        (symbol, limit),
    ).fetchall()
    return [{"date": row["date"], "close": row["close"]} for row in rows]


def get_all_history(symbol):
    """Get all historical prices for a symbol."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT date, close FROM history WHERE symbol = ? ORDER BY date ASC",
        (symbol,),
    ).fetchall()
    return [{"date": row["date"], "close": row["close"]} for row in rows]


def get_meta(key):
    """Get a metadata value."""
    conn = _get_conn()
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(key, value):
    """Set a metadata value."""
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


# --- Casualty Data ---

def save_casualty(date, category, value):
    """Save or update a casualty estimate for a given date and category."""
    import time
    conn = _get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO casualties (date, category, value, source, fetched_at)
           VALUES (?, ?, ?, 'gemini_estimate', ?)""",
        (date, category, value, int(time.time())),
    )
    conn.commit()


def get_casualties_by_category(category):
    """Get all casualty records for a category, ordered by date ascending."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT date, value FROM casualties WHERE category = ? ORDER BY date ASC",
        (category,),
    ).fetchall()
    return [{"date": row["date"], "value": row["value"]} for row in rows]


def get_all_casualties():
    """Get all casualty data grouped by category."""
    categories = [
        "us_deaths", "iran_deaths", "other_deaths",
        "us_injuries", "iran_injuries", "other_injuries",
        "displaced",
    ]
    result = {}
    for cat in categories:
        result[cat] = get_casualties_by_category(cat)
    return result


def get_casualty_totals():
    """Get the latest cumulative totals for each category."""
    conn = _get_conn()
    rows = conn.execute(
        """SELECT category, SUM(value) as total
           FROM casualties GROUP BY category""",
    ).fetchall()
    return {row["category"]: row["total"] for row in rows}


# --- Source Data ---

def save_source(url, title, source_group="general"):
    """Save a source URL. Ignores duplicates within the same group."""
    import time
    conn = _get_conn()
    conn.execute(
        """INSERT OR IGNORE INTO sources (url, title, source_group, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (url, title, source_group, int(time.time())),
    )
    conn.commit()


def get_all_sources():
    """Get all sources grouped by source_group."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT url, title, source_group, fetched_at FROM sources ORDER BY fetched_at DESC",
    ).fetchall()
    grouped = {}
    for row in rows:
        grp = row["source_group"] or "general"
        if grp not in grouped:
            grouped[grp] = []
        grouped[grp].append({"url": row["url"], "title": row["title"]})
    return grouped


def get_unresolved_sources(limit=20):
    """Get sources that haven't been resolved yet or need retry."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT s.id, s.url, s.title, s.source_group
        FROM sources s
        LEFT JOIN resolved_sources rs ON s.id = rs.source_id
        WHERE rs.source_id IS NULL
           OR (rs.status = 'pending' AND rs.attempts < 3)
        ORDER BY s.fetched_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(row) for row in rows]


def save_resolved_source(source_id, resolved_url, resolved_title, status, error=None):
    """Save or update a resolved source entry."""
    import time
    conn = _get_conn()
    conn.execute("""
        INSERT INTO resolved_sources (source_id, resolved_url, resolved_title, status, error, attempts, resolved_at, created_at)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            resolved_url = COALESCE(excluded.resolved_url, resolved_sources.resolved_url),
            resolved_title = COALESCE(excluded.resolved_title, resolved_sources.resolved_title),
            status = excluded.status,
            error = excluded.error,
            attempts = resolved_sources.attempts + 1,
            resolved_at = CASE WHEN excluded.status = 'resolved' THEN excluded.resolved_at ELSE resolved_sources.resolved_at END
    """, (
        source_id, resolved_url, resolved_title, status, error,
        int(time.time()) if status == 'resolved' else None,
        int(time.time()),
    ))
    conn.commit()


def get_all_sources_resolved():
    """Get all sources, preferring resolved data when available."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT
            COALESCE(CASE WHEN rs.status = 'resolved' THEN rs.resolved_url END, s.url) AS url,
            COALESCE(CASE WHEN rs.status = 'resolved' THEN rs.resolved_title END, s.title) AS title,
            s.source_group,
            s.fetched_at
        FROM sources s
        LEFT JOIN resolved_sources rs ON s.id = rs.source_id
        ORDER BY s.fetched_at DESC
    """).fetchall()
    grouped = {}
    for row in rows:
        grp = row["source_group"] or "general"
        if grp not in grouped:
            grouped[grp] = []
        grouped[grp].append({"url": row["url"], "title": row["title"]})
    return grouped


def get_latest_history_date(symbol):
    """Return the most recent date in history for a symbol, or None."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT MAX(date) as max_date FROM history WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    return row["max_date"] if row and row["max_date"] else None


def save_aaa_gas_prices(current_regular, month_ago_regular):
    """Save AAA national average gas prices."""
    import time
    conn = _get_conn()
    conn.execute(
        """INSERT INTO aaa_gas_prices (current_regular, month_ago_regular, fetched_at)
           VALUES (?, ?, ?)""",
        (current_regular, month_ago_regular, int(time.time())),
    )
    conn.commit()


def get_aaa_gas_prices():
    """Return the most recent AAA gas prices, or None."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT current_regular, month_ago_regular, fetched_at FROM aaa_gas_prices ORDER BY fetched_at DESC LIMIT 1"
    ).fetchone()
    if row:
        return {
            "current_regular": row["current_regular"],
            "month_ago_regular": row["month_ago_regular"],
            "fetched_at": row["fetched_at"],
        }
    return None


def clear_sources():
    """Delete all sources (used when refreshing data)."""
    conn = _get_conn()
    conn.execute("DELETE FROM sources")
    conn.commit()
