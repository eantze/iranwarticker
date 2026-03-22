"""SQLite database for storing market data, with Google Cloud Storage sync."""

import json
import logging
import os
import sqlite3
import tempfile
import threading

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(tempfile.gettempdir(), "market_data.db")
_local = threading.local()


def _get_conn():
    """Get a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
    return _local.conn


def init_db():
    """Create tables if they don't exist."""
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


# --- Google Cloud Storage Sync ---

_gcs_client = None


def _get_gcs_bucket():
    """Get the GCS bucket for syncing, or None if not configured."""
    from config import GCS_BUCKET
    if not GCS_BUCKET:
        return None

    global _gcs_client
    try:
        if _gcs_client is None:
            from google.cloud import storage
            _gcs_client = storage.Client()
        return _gcs_client.bucket(GCS_BUCKET)
    except Exception as e:
        logger.error("Failed to connect to GCS bucket '%s': %s", GCS_BUCKET, e)
        return None


def upload_to_gcs():
    """Upload the SQLite database to Google Cloud Storage."""
    bucket = _get_gcs_bucket()
    if not bucket:
        return False

    try:
        # Checkpoint the WAL so all data is in the main DB file
        conn = _get_conn()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        blob = bucket.blob("market_data.db")
        blob.upload_from_filename(DB_PATH)
        logger.info("Database uploaded to GCS")
        return True
    except Exception as e:
        logger.error("Failed to upload database to GCS: %s", e)
        return False


def download_from_gcs():
    """Download the SQLite database from Google Cloud Storage."""
    bucket = _get_gcs_bucket()
    if not bucket:
        return False

    try:
        blob = bucket.blob("market_data.db")
        if blob.exists():
            blob.download_to_filename(DB_PATH)
            logger.info("Database downloaded from GCS")
            # Reset thread-local connections so they pick up the new file
            _local.conn = None
            return True
        else:
            logger.info("No database found in GCS — starting fresh")
            return False
    except Exception as e:
        logger.error("Failed to download database from GCS: %s", e)
        return False
