"""Background data collector that fetches market data and stores it in SQLite."""

import logging
import threading
import time

import requests

from config import TWELVE_DATA_KEY, EIA_API_KEY
from services.database import (
    get_meta,
    save_history,
    save_quote,
    set_meta,
    upload_to_gcs,
)

logger = logging.getLogger(__name__)

# --- Instrument definitions ---

# Twelve Data: indices only (2 symbols = 2 credits per batch quote)
INDEX_INSTRUMENTS = {
    "sp500": {"symbol": "SPY", "label": "S&P 500", "exchange": "INDEX: SPX"},
    "dji": {"symbol": "DIA", "label": "Dow Jones Industrial", "exchange": "INDEX: DJI"},
}
INDEX_SYMBOLS = [info["symbol"] for info in INDEX_INSTRUMENTS.values()]
INDEX_SYMBOLS_STR = ",".join(INDEX_SYMBOLS)

# EIA: energy commodities (4 series, separate API — no rate limit conflict)
EIA_INSTRUMENTS = {
    "wti": {
        "series": "RWTC",
        "endpoint": "petroleum/pri/spt",
        "label": "WTI crude oil",
        "exchange": "CLW00: NYMEX",
        "unit": "$/barrel",
    },
    "brent": {
        "series": "RBRTE",
        "endpoint": "petroleum/pri/spt",
        "label": "Brent crude oil",
        "exchange": "NYMEX: BZW00",
        "unit": "$/barrel",
    },
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

QUOTE_INTERVAL = 300       # Fetch quotes every 5 minutes
HISTORY_INTERVAL = 3600    # Fetch history every 1 hour
GCS_SYNC_INTERVAL = 600    # Upload to GCS every 10 minutes


def start_collector():
    """Start the background data collection thread."""
    thread = threading.Thread(target=_collector_loop, daemon=True)
    thread.start()
    logger.info("Background data collector started")


def _collector_loop():
    """Main loop: fetch quotes frequently, history less often, sync to GCS periodically."""
    last_history_fetch = 0
    last_gcs_sync = 0

    # Initial fetch on startup
    _fetch_index_quotes()
    _fetch_eia_latest()
    _fetch_index_history()
    _fetch_eia_history()
    last_history_fetch = time.time()

    # Initial GCS upload
    upload_to_gcs()
    last_gcs_sync = time.time()

    while True:
        time.sleep(QUOTE_INTERVAL)

        # Fetch current prices
        try:
            _fetch_index_quotes()
        except Exception as e:
            logger.error("Index quote fetch error: %s", e)

        try:
            _fetch_eia_latest()
        except Exception as e:
            logger.error("EIA quote fetch error: %s", e)

        # Fetch history less often
        now = time.time()
        if now - last_history_fetch >= HISTORY_INTERVAL:
            try:
                _fetch_index_history()
                _fetch_eia_history()
                last_history_fetch = now
            except Exception as e:
                logger.error("History fetch error: %s", e)

        # Sync to GCS periodically
        if now - last_gcs_sync >= GCS_SYNC_INTERVAL:
            try:
                upload_to_gcs()
                last_gcs_sync = now
            except Exception as e:
                logger.error("GCS sync error: %s", e)


# --- Twelve Data: Index quotes and history ---

def _fetch_index_quotes():
    """Batch fetch index quotes from Twelve Data (1 API call)."""
    if not TWELVE_DATA_KEY:
        logger.warning("No TWELVE_DATA_KEY — skipping index quote fetch")
        return

    try:
        resp = requests.get(
            "https://api.twelvedata.com/quote",
            params={"symbol": INDEX_SYMBOLS_STR, "apikey": TWELVE_DATA_KEY},
            timeout=15,
        )
        if not resp.ok:
            logger.error("Twelve Data quote batch error: %s", resp.status_code)
            return

        data = resp.json()
        for symbol in INDEX_SYMBOLS:
            d = data.get(symbol, {})
            if isinstance(d, dict) and "close" in d:
                price = float(d["close"])
                prev = float(d.get("previous_close", price))
                change = round(price - prev, 2)
                change_pct = f"{(change / prev * 100) if prev else 0:+.2f}%"
                save_quote(symbol, price, change, change_pct)
            else:
                logger.warning("No quote data for %s: %s", symbol, d)

        logger.info("Index quotes updated")

    except Exception as e:
        logger.error("Index quote fetch exception: %s", e)


def _fetch_index_history():
    """Fetch daily history for index ETFs from Twelve Data."""
    if not TWELVE_DATA_KEY:
        return

    for symbol in INDEX_SYMBOLS:
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
                    logger.error("Index history API error for %s: %s", symbol, resp_json.get("message"))
                    continue
                values = resp_json.get("values", [])
                records = [
                    {"date": item["datetime"], "close": float(item["close"])}
                    for item in reversed(values)
                ]
                save_history(symbol, records)
                logger.info("Index history for %s: %d values", symbol, len(records))
            else:
                logger.error("Index history error for %s: %s", symbol, resp.status_code)
        except Exception as e:
            logger.error("Index history exception for %s: %s", symbol, e)

        time.sleep(12)  # Respect Twelve Data rate limit


# --- EIA: Energy commodity quotes and history ---

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
    """Fetch latest prices for all 4 energy commodities from EIA."""
    if not EIA_API_KEY:
        logger.warning("No EIA_API_KEY — skipping EIA quote fetch")
        return

    for key, info in EIA_INSTRUMENTS.items():
        try:
            data = _eia_fetch(info["endpoint"], info["series"], length=2)
            if data and len(data) >= 1:
                price = float(data[0]["value"])
                # Calculate day change from previous day if available
                if len(data) >= 2:
                    prev = float(data[1]["value"])
                    change = round(price - prev, 4)
                    change_pct = f"{(change / prev * 100) if prev else 0:+.2f}%"
                else:
                    change = 0
                    change_pct = "+0.00%"

                # Use the EIA series name as the symbol key in the database
                save_quote(info["series"], price, change, change_pct)
                logger.debug("EIA quote %s (%s): %.4f", key, info["series"], price)
            else:
                logger.warning("No EIA data for %s", key)
        except Exception as e:
            logger.error("EIA quote fetch error for %s: %s", key, e)

    logger.info("EIA energy quotes updated")


def _fetch_eia_history():
    """Fetch daily history for all 4 energy commodities from EIA."""
    if not EIA_API_KEY:
        return

    for key, info in EIA_INSTRUMENTS.items():
        try:
            data = _eia_fetch(info["endpoint"], info["series"], length=60)
            if data:
                # EIA returns newest first, reverse for chronological order
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
