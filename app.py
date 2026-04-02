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


@app.route('/natural-gas')
def natural_gas():
    return render_template('natural_gas.html', active_page='natural_gas')


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


@app.route('/gas-predictor')
def gas_predictor():
    from gas_predictor import get_gas_predictor_data
    from services.database import get_aaa_gas_prices
    data = get_gas_predictor_data()
    aaa = get_aaa_gas_prices()
    return render_template('gas_predictor.html', active_page='gas_predictor', data=data, aaa=aaa)


@app.route('/casualties')
def casualties():
    return render_template('casualties.html', active_page='casualties')


@app.route('/api/casualties')
def api_casualties():
    try:
        from services.database import get_all_casualties, get_casualty_totals, get_all_sources_resolved
        return jsonify({
            "totals": get_casualty_totals(),
            "daily": get_all_casualties(),
            "sources": get_all_sources_resolved(),
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
            "tyx": {"label": "30-Year Treasury Yield", "price": None, "change": None, "change_pct": None, "history": []},
            "updated_at": None,
        })


def _startup():
    """Initialize database and start background collector in a thread so the worker boots immediately."""
    import threading

    def _init():
        try:
            if not DEMO_MODE:
                from services.database import init_db
                from services.data_collector import start_collector
                from services.casualty_collector import start_casualty_collector

                # Initialize tables (creates data/ dir and tables if needed)
                init_db()

                # Start background data collection
                start_collector()

                # Start casualty data collection (Gemini)
                start_casualty_collector()

                # Start source URL resolver (beautifies redirect URLs)
                from services.source_resolver import start_source_resolver
                start_source_resolver()

                # Start AAA gas price collector
                from services.aaa_collector import start_aaa_collector
                start_aaa_collector()

            # Pre-warm gas predictor cache (works in both demo and prod)
            from gas_predictor import warm_cache
            warm_cache()

            logging.getLogger(__name__).info("Startup complete")
        except Exception as e:
            logging.getLogger(__name__).error("Startup error: %s", e)

    threading.Thread(target=_init, daemon=True).start()


# Run startup when the module loads (works with both flask run and gunicorn)
_startup()


if __name__ == '__main__':
    app.run(debug=True, port=PORT)
