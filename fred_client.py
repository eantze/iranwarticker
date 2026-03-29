"""FRED API wrapper with file-based caching."""

import json
import logging
import os
import time
from pathlib import Path

import requests

from config import FRED_API_KEY

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
CACHE_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "cache"
CACHE_TTL = 86400  # 24 hours


def fetch_series(series_id, start="1990-01-01", frequency=None, aggregation="avg", units="lin"):
    """Fetch a FRED series, using file cache with 24h TTL."""
    cache_key = f"{series_id}_{frequency}_{units}"
    cache_file = CACHE_DIR / f"{cache_key}.json"

    # Check cache
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_TTL:
            try:
                return json.loads(cache_file.read_text())
            except (json.JSONDecodeError, OSError):
                pass

    if not FRED_API_KEY:
        logger.warning("FRED_API_KEY not set, returning cached or empty data")
        if cache_file.exists():
            return json.loads(cache_file.read_text())
        return []

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start,
    }
    if frequency:
        params["frequency"] = frequency
        params["aggregation_method"] = aggregation
    if units != "lin":
        params["units"] = units

    try:
        resp = requests.get(FRED_BASE, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()["observations"]

        # Cache to disk
        CACHE_DIR.mkdir(exist_ok=True)
        cache_file.write_text(json.dumps(data))
        logger.info("Fetched %d observations for %s", len(data), series_id)
        return data
    except Exception as e:
        logger.error("FRED API error for %s: %s", series_id, e)
        # Fall back to stale cache
        if cache_file.exists():
            logger.info("Using stale cache for %s", series_id)
            return json.loads(cache_file.read_text())
        return []
