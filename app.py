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


@app.route('/casualties')
def casualties():
    return render_template('casualties.html', active_page='casualties')


@app.route('/api/casualties')
def api_casualties():
    from services.database import get_all_casualties, get_casualty_totals, get_all_sources
    return jsonify({
        "totals": get_casualty_totals(),
        "daily": get_all_casualties(),
        "sources": get_all_sources(),
    })


@app.route('/api/prices')
def api_prices():
    if DEMO_MODE:
        from services.demo_data import get_demo_prices
        return jsonify(get_demo_prices())
    else:
        from services.market_data import get_all_prices
        return jsonify(get_all_prices())


def _startup():
    """Initialize database and start background collector."""
    if not DEMO_MODE:
        from services.database import init_db, download_from_gcs
        from services.data_collector import start_collector
        from services.casualty_collector import start_casualty_collector

        # Try to restore database from GCS first
        download_from_gcs()

        # Initialize tables (no-op if they already exist)
        init_db()

        # Start background data collection
        start_collector()

        # Start casualty data collection (Gemini)
        start_casualty_collector()


# Run startup when the module loads (works with both flask run and gunicorn)
_startup()


if __name__ == '__main__':
    app.run(debug=True, port=PORT)
