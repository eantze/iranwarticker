"""Background service that resolves redirect URLs (e.g. vertexaisearch) to real article URLs."""

import logging
import re
import threading
import time

import requests

from services.database import get_unresolved_sources, save_resolved_source

logger = logging.getLogger(__name__)

RESOLVE_INTERVAL = 600  # Check every 10 minutes
MAX_ATTEMPTS = 3
REQUEST_TIMEOUT = 15
USER_AGENT = (
    "Mozilla/5.0 (compatible; IranTickerBot/1.0; +https://iranticker.com)"
)

# Domains that are Google redirect wrappers needing resolution
REDIRECT_DOMAINS = [
    "vertexaisearch.cloud.google.com",
    "googleapis.com",
]


def start_source_resolver():
    """Start the background resolver thread."""
    thread = threading.Thread(target=_resolver_loop, daemon=True)
    thread.start()
    logger.info("Source resolver started")


def _resolver_loop():
    """Main loop: wait, then periodically resolve pending sources."""
    # Initial delay — let casualty_collector populate sources first
    time.sleep(30)
    _resolve_pending()
    while True:
        time.sleep(RESOLVE_INTERVAL)
        try:
            _resolve_pending()
        except Exception as e:
            logger.error("Source resolver cycle error: %s", e)


def _resolve_pending():
    """Process a batch of unresolved sources."""
    sources = get_unresolved_sources(limit=20)
    if not sources:
        return

    logger.info("Source resolver: %d sources to process", len(sources))
    resolved_count = 0

    for src in sources:
        try:
            if _resolve_one(src):
                resolved_count += 1
        except Exception as e:
            logger.error("Resolve error for source %d: %s", src["id"], e)
            save_resolved_source(src["id"], None, None, status="pending", error=str(e)[:200])
        # Rate limit between requests
        time.sleep(2)

    if resolved_count:
        logger.info("Source resolver: resolved %d/%d sources", resolved_count, len(sources))


def _needs_resolving(url):
    """Check if a URL is a redirect wrapper that needs resolution."""
    if not url:
        return False
    for domain in REDIRECT_DOMAINS:
        if domain in url:
            return True
    return False


def _resolve_one(src):
    """Resolve a single source. Returns True if successfully resolved."""
    source_id = src["id"]
    url = src["url"]

    # If it's already a real URL, mark as skipped
    if not _needs_resolving(url):
        save_resolved_source(source_id, url, src["title"], status="skipped")
        return True

    # Follow redirects to get the final URL
    final_url = _follow_redirects(url)
    if not final_url:
        save_resolved_source(source_id, None, None, status="pending", error="Could not follow redirect")
        return False

    # Scrape the page title from the final URL
    title = _scrape_title(final_url) or src["title"]

    save_resolved_source(source_id, final_url, title, status="resolved")
    logger.info("Resolved: %s -> %s (%s)", url[:60], final_url[:60], title[:40] if title else "no title")
    return True


def _follow_redirects(url):
    """Follow redirect chain to get the final destination URL."""
    headers = {"User-Agent": USER_AGENT}

    # Try HEAD first (cheaper, no body download)
    try:
        resp = requests.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers=headers)
        if resp.status_code < 400:
            return resp.url
    except requests.RequestException:
        pass

    # Fall back to GET if HEAD fails
    try:
        resp = requests.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers=headers, stream=True)
        if resp.status_code < 400:
            return resp.url
    except requests.RequestException:
        pass

    return None


def _scrape_title(url):
    """Fetch page and extract <title> tag. Returns None on failure."""
    try:
        resp = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            stream=True,
        )
        resp.raise_for_status()
        # Only read first 64KB — <title> is always in <head>
        content = resp.raw.read(65536).decode("utf-8", errors="ignore")
        match = re.search(r"<title[^>]*>(.*?)</title>", content, re.IGNORECASE | re.DOTALL)
        if match:
            title = match.group(1).strip()
            # Clean up common artifacts
            title = re.sub(r"\s+", " ", title)  # collapse whitespace
            return title[:500]
    except Exception:
        pass
    return None
