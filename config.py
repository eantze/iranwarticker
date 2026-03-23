import os
from dotenv import load_dotenv

load_dotenv()

TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_KEY", "")
EIA_API_KEY = os.getenv("EIA_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"
PORT = int(os.getenv("PORT", 5000))
GCS_BUCKET = os.getenv("GCS_BUCKET", "")  # e.g. "oil-gas-ticker-data"
