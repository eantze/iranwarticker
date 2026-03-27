import logging

from flask import Flask, render_template, jsonify

from config import DEMO_MODE, PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html', active_page='home')


@app.route('/markets')
def markets():
    return render_template('markets.html', active_page='markets')


@app.route('/environmental-impact')
def environmental_impact():
    return render_template('environmental.html', active_page='environmental')


@app.route('/total-cost')
def total_cost():
    return render_template('total_cost.html', active_page='total_cost')


@app.route('/fertilizer')
def fertilizer():
    return render_template('fertilizer.html', active_page='fertilizer')


@app.route('/casualties')
def casualties():
    return render_template('casualties.html', active_page='casualties')


@app.route('/api/casualties')
def api_casualties():
    try:
        from services.database import get_all_casualties, get_casualty_totals, get_all_sources
        return jsonify({
            "totals": get_casualty_totals(),
            "daily": get_all_casualties(),
            "sources": get_all_sources(),
        })
    except Exception:
        return jsonify({"totals": {}, "daily": {}, "sources": {}})


@app.route('/api/prices')
def api_prices():
    if DEMO_MODE:
        return jsonify({"error": "Demo mode — no live data"})
    try:
        from services.market_data import get_all_prices
        return jsonify(get_all_prices())
    except Exception as e:
        # Return empty structure during startup before tables are ready
        return jsonify({
            "sp500": {"label": "S&P 500", "price": None, "change": None, "change_pct": None, "history": []},
            "dji": {"label": "Dow Jones Industrial", "price": None, "change": None, "change_pct": None, "history": []},
            "wti": {"label": "WTI Crude Oil Futures", "price": None, "change": None, "change_pct": None, "history": []},
            "brent": {"label": "Brent Crude Oil Futures", "price": None, "change": None, "change_pct": None, "history": []},
            "updated_at": None,
        })


def _startup():
    """Initialize database and start background collector in a thread so the worker boots immediately."""
    if not DEMO_MODE:
        import threading

        def _init():
            try:
                from services.database import init_db
                from services.data_collector import start_collector
                from services.casualty_collector import start_casualty_collector

                # Initialize tables (creates data/ dir and tables if needed)
                init_db()

                # Start background data collection
                start_collector()

                # Start casualty data collection (Gemini)
                start_casualty_collector()

                logging.getLogger(__name__).info("Startup complete")
            except Exception as e:
                logging.getLogger(__name__).error("Startup error: %s", e)

        threading.Thread(target=_init, daemon=True).start()


# Run startup when the module loads (works with both flask run and gunicorn)
_startup()


if __name__ == '__main__':
    app.run(debug=True, port=PORT)
