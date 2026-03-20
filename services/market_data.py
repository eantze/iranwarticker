"""Market data service — reads from SQLite database populated by the background collector."""

import logging
import time

from services.database import get_all_history, get_quote

logger = logging.getLogger(__name__)

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


def get_all_prices():
    """Read all instrument data from SQLite — fast, no external API calls."""
    data = {"updated_at": int(time.time()), "demo_mode": False}

    for section, instruments in INSTRUMENTS.items():
        data[section] = {}
        for key, info in instruments.items():
            symbol = info["symbol"]
            quote = get_quote(symbol)
            history = get_all_history(symbol)

            data[section][key] = {
                "label": info["label"],
                "exchange": info["exchange"],
                "price": quote["price"] if quote else None,
                "change": quote["change"] if quote else None,
                "change_pct": quote["change_pct"] if quote else None,
                "history": history,
            }

    return data
