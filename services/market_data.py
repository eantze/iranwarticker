"""Market data service — reads from SQLite database populated by the background collector."""

import logging
import time

from services.database import get_all_history, get_quote

logger = logging.getLogger(__name__)

# Indices use Twelve Data ETF symbols as DB keys
# Energy commodities use EIA series names as DB keys
INSTRUMENTS = {
    "indices": {
        "sp500": {"db_key": "SPY", "label": "S&P 500", "exchange": "INDEX: SPX"},
        "dji": {"db_key": "DIA", "label": "Dow Jones Industrial", "exchange": "INDEX: DJI"},
    },
    "crude": {
        "wti": {"db_key": "CL", "label": "WTI crude oil", "exchange": "NYMEX: CL"},
        "brent": {"db_key": "BZ", "label": "Brent crude oil", "exchange": "NYMEX: BZ"},
    },
    "fuel": {
        "rbob": {"db_key": "EER_EPMRU_PF4_RGC_DPG", "label": "RBOB gasoline", "exchange": "NYMEX: RB"},
        "ng": {"db_key": "RNGWHHD", "label": "Henry Hub natural gas", "exchange": "NYMEX: NG"},
    },
}


def get_all_prices():
    """Read all instrument data from SQLite — fast, no external API calls."""
    data = {"updated_at": int(time.time()), "demo_mode": False}

    for section, instruments in INSTRUMENTS.items():
        data[section] = {}
        for key, info in instruments.items():
            db_key = info["db_key"]
            quote = get_quote(db_key)
            history = get_all_history(db_key)

            data[section][key] = {
                "label": info["label"],
                "exchange": info["exchange"],
                "price": quote["price"] if quote else None,
                "change": quote["change"] if quote else None,
                "change_pct": quote["change_pct"] if quote else None,
                "history": history,
            }

    return data
