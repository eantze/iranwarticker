"""Background data collector that fetches market data and stores it in SQLite."""

import datetime
import logging
import threading
import time

import requests

from config import TWELVE_DATA_KEY, EIA_API_KEY
from services.database import (
    get_latest_history_date,
    get_meta,
    save_history,
    save_quote,
    set_meta,
    upload_to_gcs,
)

logger = logging.getLogger(__name__)

# --- Instrument definitions ---

# Twelve Data: indices + crude oil futures (4 symbols)
TD_INSTRUMENTS = {
    "sp500": {"symbol": "SPY", "label": "S&P 500", "exchange": "INDEX: SPX"},
    "dji": {"symbol": "DIA", "label": "Dow Jones Industrial", "exchange": "INDEX: DJI"},
    "wti": {"symbol": "CL", "label": "WTI crude oil", "exchange": "NYMEX: CL"},
    "brent": {"symbol": "BZ", "label": "Brent crude oil", "exchange": "NYMEX: BZ"},
}
TD_SYMBOLS = [info["symbol"] for info in TD_INSTRUMENTS.values()]
TD_SYMBOLS_STR = ",".join(TD_SYMBOLS)

# EIA: RBOB gasoline and natural gas only (WTI/Brent moved to Twelve Data)
EIA_INSTRUMENTS = {
    "rbob": {
        "series": "EER_EPMRU_PF4_RGC_DPG",
        "endpoint": "petroleum/pri/spt",
        "label": "RBOB gasoline",
        "exchange": "NYMEX: RB",
        "unit": "$/gallon",
    },
    "ng": {
        "series": "RNGWHHD",
        "endpoint": "natural-gas/pri/fut",
        "label": "Henry Hub natural gas",
        "exchange": "NYMEX: NG",
        "unit": "$/MMBtu",
    },
}

# Intervals
TD_QUOTE_INTERVAL = 60     # Twelve Data quotes every 1 minute
EIA_QUOTE_INTERVAL = 300   # EIA quotes every 5 minutes
HISTORY_INTERVAL = 3600    # History refresh every 1 hour
GCS_SYNC_INTERVAL = 600    # GCS upload every 10 minutes

BACKFILL_START = "2026-03-01"


def start_collector():
    """Start background data collection: backfill first, then polling threads."""
    thread = threading.Thread(target=_run_collector, daemon=True)
    thread.start()
    logger.info("Background data collector starting")


def _run_collector():
    """Backfill history, then start independent polling loops."""
    # Phase 1: Backfill any missing Twelve Data history
    try:
        _backfill_td_history()
    except Exception as e:
        logger.error("Backfill error: %s", e)

    # Phase 2: Initial EIA fetch
    try:
        _fetch_eia_latest()
        _fetch_eia_history()
    except Exception as e:
        logger.error("Initial EIA fetch error: %s", e)

    # Initial GCS sync
    try:
        upload_to_gcs()
    except Exception as e:
        logger.error("Initial GCS sync error: %s", e)

    # Phase 3: Start independent polling threads
    threading.Thread(target=_td_loop, daemon=True).start()
    threading.Thread(target=_eia_loop, daemon=True).start()
    threading.Thread(target=_gcs_sync_loop, daemon=True).start()
    logger.info("All collector threads started")


# --- Backfill Logic ---

def _backfill_td_history():
    """On startup, fill any gaps in Twelve Data history from BACKFILL_START to today."""
    if not TWELVE_DATA_KEY:
        logger.warning("No TWELVE_DATA_KEY — skipping backfill")
        return

    today = datetime.date.today().isoformat()

    for key, info in TD_INSTRUMENTS.items():
        symbol = info["symbol"]
        latest = get_latest_history_date(symbol)

        if latest and latest >= today:
            logger.info("History for %s is current (latest=%s), skipping backfill", symbol, latest)
            continue

        start = latest if latest and latest > BACKFILL_START else BACKFILL_START
        logger.info("Backfilling %s from %s to %s", symbol, start, today)

        try:
            resp = requests.get(
                "https://api.twelvedata.com/time_series",
                params={
                    "symbol": symbol,
                    "interval": "1day",
                    "start_date": start,
                    "end_date": today,
                    "outputsize": "60",
                    "apikey": TWELVE_DATA_KEY,
                },
                timeout=15,
            )
            if resp.ok:
                resp_json = resp.json()
                if resp_json.get("status") == "error":
                    logger.error("Backfill API error for %s: %s", symbol, resp_json.get("message"))
                    continue
                values = resp_json.get("values", [])
                records = [
                    {"date": item["datetime"], "close": float(item["close"])}
                    for item in reversed(values)
                ]
                save_history(symbol, records)
                logger.info("Backfilled %s: %d records", symbol, len(records))
            else:
                logger.error("Backfill HTTP error for %s: %s", symbol, resp.status_code)
        except Exception as e:
            logger.error("Backfill exception for %s: %s", symbol, e)

        time.sleep(10)  # Rate limit: stay well under 8 calls/min


# --- Twelve Data Polling Loop (every 60 seconds) ---

def _td_loop():
    """Fetch Twelve Data quotes every minute, history every hour."""
    last_history = time.time()  # Skip immediate history since backfill just ran

    while True:
        try:
            _fetch_td_quotes()
        except Exception as e:
            logger.error("TD quote fetch error: %s", e)

        # Refresh history every hour
        now = time.time()
        if now - last_history >= HISTORY_INTERVAL:
            try:
                _fetch_td_history()
                last_history = now
            except Exception as e:
                logger.error("TD history fetch error: %s", e)

        time.sleep(TD_QUOTE_INTERVAL)


def _fetch_td_quotes():
    """Batch fetch quotes for all 4 Twelve Data symbols (4 API credits)."""
    if not TWELVE_DATA_KEY:
        return

    resp = requests.get(
        "https://api.twelvedata.com/quote",
        params={"symbol": TD_SYMBOLS_STR, "apikey": TWELVE_DATA_KEY},
        timeout=15,
    )
    if not resp.ok:
        logger.error("Twelve Data quote batch error: %s", resp.status_code)
        return

    data = resp.json()
    for symbol in TD_SYMBOLS:
        d = data.get(symbol, {})
        if isinstance(d, dict) and "close" in d:
            price = float(d["close"])
            prev = float(d.get("previous_close", price))
            change = round(price - prev, 2)
            change_pct = f"{(change / prev * 100) if prev else 0:+.2f}%"
            save_quote(symbol, price, change, change_pct)
        else:
            logger.warning("No quote data for %s: %s", symbol, d)

    logger.info("Twelve Data quotes updated (SPY, DIA, CL, BZ)")


def _fetch_td_history():
    """Fetch daily history for all 4 Twelve Data symbols."""
    if not TWELVE_DATA_KEY:
        return

    for symbol in TD_SYMBOLS:
        try:
            resp = requests.get(
                "https://api.twelvedata.com/time_series",
                params={
                    "symbol": symbol,
                    "interval": "1day",
                    "outputsize": "30",
                    "apikey": TWELVE_DATA_KEY,
                },
                timeout=10,
            )
            if resp.ok:
                resp_json = resp.json()
                if resp_json.get("status") == "error":
                    logger.error("TD history API error for %s: %s", symbol, resp_json.get("message"))
                    continue
                values = resp_json.get("values", [])
                records = [
                    {"date": item["datetime"], "close": float(item["close"])}
                    for item in reversed(values)
                ]
                save_history(symbol, records)
                logger.info("TD history for %s: %d values", symbol, len(records))
            else:
                logger.error("TD history error for %s: %s", symbol, resp.status_code)
        except Exception as e:
            logger.error("TD history exception for %s: %s", symbol, e)

        time.sleep(10)  # Rate limit: ~6 calls/min leaves headroom


# --- EIA Polling Loop (every 5 minutes) ---

def _eia_loop():
    """Fetch EIA data every 5 minutes, history every hour."""
    last_history = time.time()  # Skip immediate since we fetched on startup

    while True:
        time.sleep(EIA_QUOTE_INTERVAL)

        try:
            _fetch_eia_latest()
        except Exception as e:
            logger.error("EIA quote fetch error: %s", e)

        now = time.time()
        if now - last_history >= HISTORY_INTERVAL:
            try:
                _fetch_eia_history()
                last_history = now
            except Exception as e:
                logger.error("EIA history fetch error: %s", e)


def _eia_fetch(endpoint, series, length=1):
    """Generic EIA API v2 data fetch."""
    url = f"https://api.eia.gov/v2/{endpoint}/data/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": series,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": str(length),
    }
    resp = requests.get(url, params=params, timeout=15)
    if resp.ok:
        return resp.json().get("response", {}).get("data", [])
    else:
        logger.error("EIA API error for %s: %s", series, resp.status_code)
        return []


def _fetch_eia_latest():
    """Fetch latest prices for RBOB and Natural Gas from EIA."""
    if not EIA_API_KEY:
        logger.warning("No EIA_API_KEY — skipping EIA quote fetch")
        return

    for key, info in EIA_INSTRUMENTS.items():
        try:
            data = _eia_fetch(info["endpoint"], info["series"], length=2)
            if data and len(data) >= 1:
                price = float(data[0]["value"])
                if len(data) >= 2:
                    prev = float(data[1]["value"])
                    change = round(price - prev, 4)
                    change_pct = f"{(change / prev * 100) if prev else 0:+.2f}%"
                else:
                    change = 0
                    change_pct = "+0.00%"

                save_quote(info["series"], price, change, change_pct)
                logger.debug("EIA quote %s (%s): %.4f", key, info["series"], price)
            else:
                logger.warning("No EIA data for %s", key)
        except Exception as e:
            logger.error("EIA quote fetch error for %s: %s", key, e)

    logger.info("EIA energy quotes updated")


def _fetch_eia_history():
    """Fetch daily history for RBOB and Natural Gas from EIA."""
    if not EIA_API_KEY:
        return

    for key, info in EIA_INSTRUMENTS.items():
        try:
            data = _eia_fetch(info["endpoint"], info["series"], length=60)
            if data:
                records = [
                    {"date": item["period"], "close": float(item["value"])}
                    for item in reversed(data)
                    if item.get("value") is not None
                ]
                save_history(info["series"], records)
                logger.info("EIA history for %s: %d values", key, len(records))
            else:
                logger.warning("No EIA history for %s", key)
        except Exception as e:
            logger.error("EIA history fetch error for %s: %s", key, e)

    logger.info("EIA energy history updated")


# --- GCS Sync Loop (every 10 minutes) ---

def _gcs_sync_loop():
    """Periodically upload the database to GCS."""
    while True:
        time.sleep(GCS_SYNC_INTERVAL)
        try:
            upload_to_gcs()
        except Exception as e:
            logger.error("GCS sync error: %s", e)
