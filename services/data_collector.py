"""Background data collector that fetches market data from Yahoo Finance, stores in SQLite."""

import logging
import threading
import time

from services.database import (
    save_history,
    save_quote,
    upload_to_gcs,
)

logger = logging.getLogger(__name__)

# Yahoo Finance symbols
YF_INSTRUMENTS = {
    "sp500": {"symbol": "^GSPC", "label": "S&P 500"},
    "dji": {"symbol": "^DJI", "label": "Dow Jones Industrial"},
    "wti": {"symbol": "CL=F", "label": "WTI Crude Oil Futures"},
    "brent": {"symbol": "BZ=F", "label": "Brent Crude Oil Futures"},
}

# Intervals
YF_QUOTE_INTERVAL = 300    # Yahoo Finance quotes every 5 minutes
HISTORY_INTERVAL = 3600    # History refresh every 1 hour
GCS_SYNC_INTERVAL = 600    # GCS upload every 10 minutes

HISTORY_START = "2026-01-01"


def start_collector():
    """Start background data collection threads."""
    thread = threading.Thread(target=_run_collector, daemon=True)
    thread.start()
    logger.info("Background data collector starting")


def _run_collector():
    """Start polling loops immediately, fetch initial data in parallel."""
    # Start polling loops right away so quotes begin updating
    threading.Thread(target=_yf_loop, daemon=True).start()
    threading.Thread(target=_gcs_sync_loop, daemon=True).start()
    logger.info("All collector threads started")

    # Fetch initial history (can be slow, but loops are already running)
    try:
        _fetch_yf_history()
    except Exception as e:
        logger.error("Initial YF history fetch error: %s", e)

    # Fetch initial quotes
    try:
        _fetch_yf_quotes()
    except Exception as e:
        logger.error("Initial YF quote fetch error: %s", e)

    # Initial GCS sync
    try:
        upload_to_gcs()
    except Exception as e:
        logger.error("Initial GCS sync error: %s", e)


# --- Yahoo Finance ---

def _fetch_yf_history():
    """Fetch daily history from Jan 1 2026 to present for all Yahoo Finance symbols using yfinance."""
    try:
        import yfinance as yf

        symbols = [info["symbol"] for info in YF_INSTRUMENTS.values()]
        logger.info("Fetching YF history for %s from %s", symbols, HISTORY_START)

        data = yf.download(
            tickers=symbols,
            start=HISTORY_START,
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            timeout=30,
        )

        if data is None or data.empty:
            logger.warning("No YF history data returned")
            return

        for key, info in YF_INSTRUMENTS.items():
            symbol = info["symbol"]
            try:
                if len(symbols) > 1:
                    ticker_data = data[symbol]
                else:
                    ticker_data = data

                # Drop NaN rows
                ticker_data = ticker_data.dropna(subset=["Close"])

                records = []
                for date_idx, row in ticker_data.iterrows():
                    date_str = date_idx.strftime("%Y-%m-%d")
                    records.append({
                        "date": date_str,
                        "close": float(row["Close"]),
                        "open": float(row["Open"]) if "Open" in row else None,
                        "high": float(row["High"]) if "High" in row else None,
                        "low": float(row["Low"]) if "Low" in row else None,
                        "volume": float(row["Volume"]) if "Volume" in row else None,
                    })

                save_history(key, records)
                logger.info("YF history for %s (%s): %d records", key, symbol, len(records))
            except Exception as e:
                logger.error("YF history parse error for %s: %s", symbol, e)

    except Exception as e:
        logger.error("YF history fetch exception: %s", e)


def _fetch_yf_quotes():
    """Fetch current quotes for all Yahoo Finance symbols."""
    try:
        import yfinance as yf

        for key, info in YF_INSTRUMENTS.items():
            try:
                ticker = yf.Ticker(info["symbol"])
                ticker.session.timeout = 15
                fast = ticker.fast_info

                price = fast.last_price
                prev = fast.previous_close

                if price is not None and prev is not None:
                    change = round(price - prev, 2)
                    change_pct = f"{(change / prev * 100) if prev else 0:+.2f}%"
                    save_quote(key, price, change, change_pct)
                    logger.debug("YF quote %s: %.2f", key, price)
                else:
                    logger.warning("No YF quote for %s", key)
            except Exception as e:
                logger.error("YF quote error for %s: %s", key, e)

            time.sleep(2)  # Small delay between individual ticker calls

        logger.info("Yahoo Finance quotes updated")

    except Exception as e:
        logger.error("YF quote fetch exception: %s", e)


# --- Yahoo Finance Polling Loop ---

def _yf_loop():
    """Fetch Yahoo Finance quotes every 5 minutes, history every hour."""
    last_history = time.time()

    while True:
        try:
            _fetch_yf_quotes()
        except Exception as e:
            logger.error("YF quote loop error: %s", e)

        now = time.time()
        if now - last_history >= HISTORY_INTERVAL:
            try:
                _fetch_yf_history()
                last_history = now
            except Exception as e:
                logger.error("YF history loop error: %s", e)

        time.sleep(YF_QUOTE_INTERVAL)


# --- GCS Sync Loop ---

def _gcs_sync_loop():
    """Periodically upload the database to GCS."""
    while True:
        time.sleep(GCS_SYNC_INTERVAL)
        try:
            upload_to_gcs()
        except Exception as e:
            logger.error("GCS sync error: %s", e)
