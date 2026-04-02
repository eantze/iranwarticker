"""Background collector that scrapes national average gas prices from AAA."""

import logging
import re
import threading
import time

import requests

from services.database import save_aaa_gas_prices

logger = logging.getLogger(__name__)

AAA_URL = "https://gasprices.aaa.com/"
COLLECT_INTERVAL = 6 * 3600  # 6 hours
REQUEST_TIMEOUT = 20


def start_aaa_collector():
    """Start background AAA gas price collector."""
    thread = threading.Thread(target=_run_collector, daemon=True)
    thread.start()
    logger.info("AAA gas price collector starting")


def _run_collector():
    """Fetch AAA prices on startup, then every COLLECT_INTERVAL."""
    time.sleep(5)  # brief delay on startup
    while True:
        try:
            _fetch_aaa_prices()
        except Exception as e:
            logger.error("AAA collector error: %s", e)
        time.sleep(COLLECT_INTERVAL)


def _fetch_aaa_prices():
    """Scrape AAA gas prices page for national average regular prices."""
    logger.info("AAA collector: fetching gas prices...")
    resp = requests.get(AAA_URL, timeout=REQUEST_TIMEOUT, headers={
        "User-Agent": "Mozilla/5.0 (compatible; IranWarTicker/1.0)"
    })
    resp.raise_for_status()
    html = resp.text

    # Table structure: rows like <tr><td>Current Avg.</td><td>$4.081</td>...
    # First <td> price in each row is the Regular grade value
    rows = re.findall(
        r'<tr>\s*<td>(.*?)</td>\s*<td>\$([\d.]+)</td>',
        html, re.DOTALL,
    )

    current_regular = None
    month_ago_regular = None
    for label, price in rows:
        label_clean = label.strip().lower()
        if "current" in label_clean:
            current_regular = float(price)
        elif "month ago" in label_clean:
            month_ago_regular = float(price)

    if current_regular is None or month_ago_regular is None:
        logger.warning("AAA collector: could not parse prices (current=%s, month_ago=%s)", current_regular, month_ago_regular)
        return

    save_aaa_gas_prices(current_regular, month_ago_regular)
    logger.info(
        "AAA collector: saved prices - current=$%.3f, month_ago=$%.3f",
        current_regular, month_ago_regular,
    )
