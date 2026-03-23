"""Background collector that uses Gemini to estimate daily casualty figures."""

import json
import logging
import threading
import time
from datetime import datetime, timedelta

import requests

from config import GEMINI_API_KEY
from services.database import save_casualty, save_source, get_meta, set_meta, upload_to_gcs

logger = logging.getLogger(__name__)

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-pro-preview:generateContent"
GEMINI_SOURCE_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro-preview:generateContent"
CASUALTY_INTERVAL = 86400  # Re-check once daily

CATEGORIES = [
    "us_deaths",
    "iran_deaths",
    "other_deaths",
    "us_injuries",
    "iran_injuries",
    "other_injuries",
    "displaced",
]

PROMPT_TEMPLATE = """You are a military conflict analyst. The US-Iran war began in late February 2026 with Operation Epic Fury.

Based on what is known about this conflict, provide your best estimated DAILY casualty figures for EACH date from {start_date} through {end_date}.

Known context:
- The war began ~Feb 28, 2026 with joint US-Israeli airstrikes on Iran
- Iran retaliated with missile and drone strikes on US bases in the Gulf region
- As of late March 2026, approximately 13 US service members have been killed and over 230 wounded
- Iranian casualties have been significantly higher due to the air campaign
- Civilian displacement has occurred in Iran and neighboring countries
- Other regional casualties include those in Gulf states hit by Iranian strikes

Distribute the known totals realistically across the days. For categories where totals are uncertain, provide reasonable estimates consistent with the scale and nature of the conflict.

For each date, provide these categories:
- us_deaths: US military deaths that day
- iran_deaths: Iranian military and civilian deaths that day
- other_deaths: Deaths of other nationalities in the region related to this conflict
- us_injuries: US military injuries that day
- iran_injuries: Iranian military and civilian injuries that day
- other_injuries: Injuries to other nationalities in the region
- displaced: Number of people newly displaced that day

Return ONLY valid JSON in this exact format, no other text:
{{
  "estimates": [
    {{
      "date": "YYYY-MM-DD",
      "us_deaths": <number>,
      "iran_deaths": <number>,
      "other_deaths": <number>,
      "us_injuries": <number>,
      "iran_injuries": <number>,
      "other_injuries": <number>,
      "displaced": <number>
    }}
  ]
}}

Use integer values only. Do not return zeros for all fields — provide your best estimates."""

# Source-fetching prompts per group
SOURCE_PROMPTS = {
    "deaths": (
        "Find news articles and reports about deaths and fatalities in the US-Iran war "
        "(Operation Epic Fury) between {start_date} and {end_date}. "
        "Include reports on US military deaths, Iranian military deaths, Iranian civilian deaths, "
        "and any other fatalities related to the conflict. "
        "Provide specific article titles and sources."
    ),
    "injuries": (
        "Find news articles and reports about injuries and wounded personnel in the US-Iran war "
        "(Operation Epic Fury) between {start_date} and {end_date}. "
        "Include reports on US military injuries, Iranian military and civilian injuries, "
        "and medical response to the conflict. "
        "Provide specific article titles and sources."
    ),
    "displaced": (
        "Find news articles and reports about civilian displacement and refugees from the US-Iran war "
        "(Operation Epic Fury) between {start_date} and {end_date}. "
        "Include reports on people displaced within Iran, refugees fleeing to neighboring countries, "
        "and humanitarian impact of the conflict. "
        "Provide specific article titles and sources."
    ),
}


def start_casualty_collector():
    """Start the background casualty data collection thread."""
    if not GEMINI_API_KEY:
        logger.warning("No GEMINI_API_KEY — casualty collector not started")
        return
    thread = threading.Thread(target=_casualty_loop, daemon=True)
    thread.start()
    logger.info("Casualty data collector started")


def _casualty_loop():
    """Main loop: fetch missing casualty data, then re-check hourly."""
    # Initial fetch — fill in any missing days
    _fetch_missing_days()

    while True:
        time.sleep(CASUALTY_INTERVAL)
        try:
            _fetch_missing_days()
        except Exception as e:
            logger.error("Casualty fetch error: %s", e)


def _fetch_missing_days():
    """Determine which dates are missing and fetch estimates from Gemini."""
    from services.database import get_casualties_by_category

    epoch = datetime(2026, 3, 1)
    today = datetime.utcnow()

    # Don't fetch for future dates or before the epoch
    if today < epoch:
        logger.info("Before conflict epoch — no casualty data to fetch")
        return

    end_date = min(today, datetime.utcnow()).date()
    start_date = epoch.date()

    # Check which dates we already have (use us_deaths as the reference)
    existing = get_casualties_by_category("us_deaths")
    existing_dates = {r["date"] for r in existing}

    # Find missing dates
    missing_dates = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in existing_dates:
            missing_dates.append(date_str)
        current += timedelta(days=1)

    if not missing_dates:
        logger.info("Casualty data is up to date — no missing days")
        return

    logger.info("Fetching casualty estimates for %d missing days", len(missing_dates))

    # Batch into chunks of 7 days to keep Gemini responses manageable
    for i in range(0, len(missing_dates), 7):
        chunk = missing_dates[i:i + 7]
        try:
            _fetch_chunk(chunk[0], chunk[-1])
        except Exception as e:
            logger.error("Gemini fetch error for %s to %s: %s", chunk[0], chunk[-1], e)
        # Rate limit: wait between Gemini calls
        time.sleep(5)


def _fetch_chunk(start_date, end_date):
    """Fetch casualty estimates from Gemini for a date range."""
    prompt = PROMPT_TEMPLATE.format(start_date=start_date, end_date=end_date)

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}, {"url_context": {}}],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 4096,
        },
    }

    resp = requests.post(
        f"{GEMINI_URL}?key={GEMINI_API_KEY}",
        json=payload,
        timeout=120,
    )

    if not resp.ok:
        logger.error("Gemini API error: %s — %s", resp.status_code, resp.text[:200])
        return

    data = resp.json()

    # Extract text from Gemini response (may have multiple parts with grounding)
    try:
        parts = data["candidates"][0]["content"]["parts"]
        text = ""
        for part in parts:
            if "text" in part:
                text += part["text"]
        if not text:
            raise KeyError("No text parts found")
    except (KeyError, IndexError):
        logger.error("Unexpected Gemini response structure: %s", str(data)[:200])
        return

    # Parse JSON from the response (handle markdown code blocks)
    text = text.strip()
    if text.startswith("```"):
        # Remove markdown code fences
        lines = text.split("\n")
        text = "\n".join(lines[1:-1])

    try:
        result = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse Gemini JSON: %s — Response: %s", e, text[:300])
        return

    estimates = result.get("estimates", [])
    if not estimates:
        logger.warning("No estimates in Gemini response")
        return

    # Save each day's data
    for day in estimates:
        date_str = day.get("date")
        if not date_str:
            continue
        for cat in CATEGORIES:
            value = day.get(cat)
            if value is not None:
                save_casualty(date_str, cat, int(value))

    # Fetch sources grouped by topic (deaths, injuries, displaced)
    for group in ["deaths", "injuries", "displaced"]:
        try:
            _fetch_sources_for_group(start_date, end_date, group)
        except Exception as e:
            logger.warning("Source fetch error for group '%s': %s", group, e)
        time.sleep(3)  # Rate limit between source calls

    logger.info("Saved casualty estimates for %s to %s (%d days)", start_date, end_date, len(estimates))


def _fetch_sources_for_group(start_date, end_date, group):
    """Fetch source articles from Gemini for a specific topic group.
    Uses google_search + url_context tools for better source URLs."""
    prompt_template = SOURCE_PROMPTS.get(group)
    if not prompt_template:
        return

    prompt = prompt_template.format(start_date=start_date, end_date=end_date)

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}, {"url_context": {}}],
    }

    resp = requests.post(
        f"{GEMINI_SOURCE_URL}?key={GEMINI_API_KEY}",
        json=payload,
        timeout=120,
    )

    if not resp.ok:
        logger.warning("Source fetch API error for '%s': %s — %s", group, resp.status_code, resp.text[:200])
        return

    data = resp.json()
    candidate = data.get("candidates", [{}])[0]
    grounding = candidate.get("groundingMetadata", {})

    chunks = grounding.get("groundingChunks", [])
    saved = 0
    for chunk in chunks:
        web = chunk.get("web", {})
        url = web.get("uri", "")
        title = web.get("title", "")
        if url:
            save_source(url, title, source_group=group)
            saved += 1

    if saved:
        logger.info("Saved %d source URLs for group '%s' (%s to %s)", saved, group, start_date, end_date)
    else:
        logger.info("No grounding sources for group '%s' (%s to %s). Keys: %s",
                    group, start_date, end_date, list(grounding.keys()))
