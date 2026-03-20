"""Background data collector that fetches market data and stores it in SQLite."""

import logging
import threading
import time

import requests

from config import TWELVE_DATA_KEY
from services.database import (
    get_meta,
    save_history,
    save_quote,
    set_meta,
    upload_to_gcs,
)

logger = logging.getLogger(__name__)

# Same instruments as market_data.py
INSTRUMENTS = {
    "indices": {
        "sp500": {"symbol": "SPY", "label": "S&P 500", "exchange": "INDEX: SPX"},
        "dji": {"symbol": "DIA", "label": "Dow Jones Industrial", "exchange": "INDEX: DJI"},
    },
    "crude": {
        "wti": {"symbol": "USO", "label": "WTI crude oil futures", "exchange": "NYMEX: CL"},
        "brent": {"symbol": "BNO", "label": "Brent crude oil futures", "exchange": "NYMEX: BZW00"},
    },
    "fuel": {
        "rbob": {"symbol": "UGA", "label": "RBOB gasoline futures", "exchange": "NYMEX: RB"},
        "ng": {"symbol": "UNG", "label": "Henry Hub natural gas", "exchange": "NYMEX: NG"},
    },
}

ALL_SYMBOLS = [info["symbol"] for insts in INSTRUMENTS.values() for info in insts.values()]
ALL_SYMBOLS_STR = ",".join(ALL_SYMBOLS)

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
    # Batch quote uses ~6 credits (1 per symbol), so wait for rate limit reset
    _fetch_and_store_quotes()
    logger.info("Waiting 60s for rate limit reset before fetching history...")
    time.sleep(60)
    _fetch_and_store_all_history()
    last_history_fetch = time.time()

    # Initial GCS upload
    upload_to_gcs()
    last_gcs_sync = time.time()

    while True:
        time.sleep(QUOTE_INTERVAL)

        # Always fetch quotes
        try:
            _fetch_and_store_quotes()
        except Exception as e:
            logger.error("Quote fetch error: %s", e)

        # Fetch history less often
        now = time.time()
        if now - last_history_fetch >= HISTORY_INTERVAL:
            try:
                _fetch_and_store_all_history()
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


def _fetch_and_store_quotes():
    """Batch fetch quotes for all symbols and store in SQLite."""
    if not TWELVE_DATA_KEY:
        logger.warning("No TWELVE_DATA_KEY — skipping quote fetch")
        return

    try:
        resp = requests.get(
            "https://api.twelvedata.com/quote",
            params={"symbol": ALL_SYMBOLS_STR, "apikey": TWELVE_DATA_KEY},
            timeout=15,
        )
        if not resp.ok:
            logger.error("Twelve Data quote batch error: %s", resp.status_code)
            return

        data = resp.json()
        for symbol in ALL_SYMBOLS:
            d = data.get(symbol, {})
            if isinstance(d, dict) and "close" in d:
                price = float(d["close"])
                prev = float(d.get("previous_close", price))
                change = round(price - prev, 2)
                change_pct = f"{(change / prev * 100) if prev else 0:+.2f}%"
                save_quote(symbol, price, change, change_pct)
                logger.debug("Saved quote: %s = %.2f", symbol, price)
            else:
                logger.warning("No quote data for %s: %s", symbol, d)

        set_meta("last_quote_fetch", str(int(time.time())))
        logger.info("Quotes updated for all symbols")

    except Exception as e:
        logger.error("Quote fetch exception: %s", e)


def _fetch_and_store_all_history():
    """Fetch daily history for each symbol and store in SQLite."""
    if not TWELVE_DATA_KEY:
        logger.warning("No TWELVE_DATA_KEY — skipping history fetch")
        return

    for symbol in ALL_SYMBOLS:
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
                    logger.error("History API error for %s: %s", symbol, resp_json.get("message", resp_json))
                    time.sleep(8)
                    continue
                values = resp_json.get("values", [])
                logger.info("History fetch for %s: got %d values", symbol, len(values))
                records = []
                for item in values:
                    records.append({
                        "date": item["datetime"],
                        "close": float(item["close"]),
                        "open": float(item.get("open", 0)),
                        "high": float(item.get("high", 0)),
                        "low": float(item.get("low", 0)),
                        "volume": float(item.get("volume", 0)),
                    })
                save_history(symbol, records)
                logger.debug("Saved %d history records for %s", len(records), symbol)
            else:
                logger.error("History fetch error for %s: %s", symbol, resp.status_code)

        except Exception as e:
            logger.error("History fetch exception for %s: %s", symbol, e)

        # Respect rate limit: 8 requests/min on free tier
        # 12s between calls = 5 calls/min, safely under the limit
        time.sleep(12)

    set_meta("last_history_fetch", str(int(time.time())))
    logger.info("History updated for all symbols")
