"""Market data service — reads from SQLite database populated by the background collector."""

import logging
import time

from services.database import get_all_history, get_quote

logger = logging.getLogger(__name__)

# DB keys match the collector's key names
INSTRUMENTS = {
    "sp500": {"db_key": "sp500", "label": "S&P 500", "symbol": "^GSPC"},
    "dji": {"db_key": "dji", "label": "Dow Jones Industrial", "symbol": "^DJI"},
    "wti": {"db_key": "wti", "label": "WTI Crude Oil Futures", "symbol": "CL=F"},
    "brent": {"db_key": "brent", "label": "Brent Crude Oil Futures", "symbol": "BZ=F"},
    "tyx": {"db_key": "tyx", "label": "30-Year Treasury Yield", "symbol": "^TYX"},
}


def get_all_prices():
    """Read all instrument data from SQLite — fast, no external API calls."""
    data = {"updated_at": int(time.time())}

    for key, info in INSTRUMENTS.items():
        db_key = info["db_key"]
        quote = get_quote(db_key)
        history = get_all_history(db_key)

        data[key] = {
            "label": info["label"],
            "symbol": info["symbol"],
            "price": quote["price"] if quote else None,
            "change": quote["change"] if quote else None,
            "change_pct": quote["change_pct"] if quote else None,
            "history": history,
        }

    return data
