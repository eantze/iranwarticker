import math
import random
import time


def _generate_history(base_price, target_price, volatility_pct, hours):
    """Generate realistic price history trending from base to target."""
    history = []
    epoch = 1772323200  # 2026-03-01T00:00:00Z
    now = int(time.time())
    start = max(epoch, now - hours * 3600)

    if hours < 2:
        return history

    price = base_price
    step_trend = (target_price - base_price) / hours

    for i in range(hours):
        ts = start + i * 3600
        if ts > now:
            break

        noise = random.gauss(0, base_price * volatility_pct)
        price = base_price + step_trend * i + noise
        # Clamp to reasonable range around the trend line
        trend_val = base_price + step_trend * i
        price = max(trend_val * 0.92, min(trend_val * 1.08, price))

        high = price * (1 + abs(random.gauss(0, volatility_pct * 0.3)))
        low = price * (1 - abs(random.gauss(0, volatility_pct * 0.3)))

        history.append({
            "open": round(price - noise * 0.1, 4),
            "high": round(high, 4),
            "low": round(low, 4),
            "close": round(price, 4),
            "volume": random.randint(500, 5000),
            "timestamp": ts,
        })

    return history


def _generate_daily_history(base_price, target_price, volatility_pct, days):
    """Generate daily close prices for index sparklines."""
    history = []
    epoch = 1772323200
    now = int(time.time())

    step_trend = (target_price - base_price) / max(days, 1)

    for i in range(days):
        ts = epoch + i * 86400
        if ts > now:
            break
        date_str = time.strftime("%Y-%m-%d", time.gmtime(ts))
        noise = random.gauss(0, base_price * volatility_pct)
        price = base_price + step_trend * i + noise
        trend_val = base_price + step_trend * i
        price = max(trend_val * 0.95, min(trend_val * 1.05, price))

        history.append({
            "date": date_str,
            "close": round(price, 2),
        })

    return history


def get_demo_prices():
    """Return simulated market data for all 6 instruments."""
    random.seed(int(time.time()) // 300)  # Changes every 5 minutes

    now = int(time.time())
    epoch = 1772323200
    days_since = max(0, (now - epoch) / 86400)
    hours_since = int(days_since * 24)

    # S&P 500 - dropped ~8% due to conflict uncertainty
    sp500_price = 5750 - days_since * 12 + random.gauss(0, 15)
    sp500_change = random.gauss(-5, 12)

    # Dow Jones - dropped ~7%
    dji_price = 42500 - days_since * 75 + random.gauss(0, 60)
    dji_change = random.gauss(-30, 70)

    # WTI Crude - spiked from $68 to ~$105 range
    wti_price = 68 + days_since * 1.8 + random.gauss(0, 1.2)
    wti_price = min(wti_price, 115)

    # Brent Crude - spiked from $72 to ~$110 range
    brent_price = 72 + days_since * 1.9 + random.gauss(0, 1.2)
    brent_price = min(brent_price, 120)

    # RBOB Gasoline - up from $2.10 to ~$3.30
    rbob_price = 2.10 + days_since * 0.055 + random.gauss(0, 0.02)
    rbob_price = min(rbob_price, 3.80)

    # Natural Gas - up from $3.50 to ~$5.50
    ng_price = 3.50 + days_since * 0.09 + random.gauss(0, 0.06)
    ng_price = min(ng_price, 6.50)

    return {
        "indices": {
            "sp500": {
                "label": "S&P 500",
                "exchange": "INDEX: SPX",
                "price": round(sp500_price, 2),
                "change": round(sp500_change, 2),
                "change_pct": f"{sp500_change / sp500_price * 100:+.2f}%",
                "history": _generate_daily_history(5750, sp500_price, 0.005, int(days_since) + 1),
            },
            "dji": {
                "label": "Dow Jones Industrial",
                "exchange": "INDEX: DJI",
                "price": round(dji_price, 2),
                "change": round(dji_change, 2),
                "change_pct": f"{dji_change / dji_price * 100:+.2f}%",
                "history": _generate_daily_history(42500, dji_price, 0.004, int(days_since) + 1),
            },
        },
        "crude": {
            "wti": {
                "label": "WTI crude oil futures",
                "exchange": "CLW00: NYMEX",
                "price": round(wti_price, 2),
                "updated": now,
                "history": _generate_history(68, wti_price, 0.006, min(hours_since, 480)),
            },
            "brent": {
                "label": "Brent crude oil futures",
                "exchange": "NYMEX: BZW00",
                "price": round(brent_price, 2),
                "updated": now,
                "history": _generate_history(72, brent_price, 0.006, min(hours_since, 480)),
            },
        },
        "fuel": {
            "rbob": {
                "label": "RBOB gasoline futures",
                "exchange": "NYMEX: RB",
                "price": round(rbob_price, 4),
                "updated": now,
                "history": _generate_history(2.10, rbob_price, 0.008, min(hours_since, 480)),
            },
            "ng": {
                "label": "Henry Hub natural gas",
                "exchange": "NYMEX: NG",
                "price": round(ng_price, 4),
                "updated": now,
                "history": _generate_history(3.50, ng_price, 0.01, min(hours_since, 480)),
            },
        },
        "updated_at": now,
        "demo_mode": True,
    }
