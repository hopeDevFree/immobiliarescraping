"""Read the unchanged public search API through Jina Reader.

Supports optional JINA_API_KEY environment variable for dedicated rate limits.
Includes automatic retries for transient 429 rate limits.
"""

import json
import logging
import os
import re
import time
from types import SimpleNamespace

from access_http import HttpScanError, _captcha_object, _page_url, diagnostic_targets, parse_search_json

logger = logging.getLogger(__name__)

READER_PREFIX = "https://r.jina.ai/"
READER_TIMEOUT = 30
READER_TRANSPORTS = ("curl", "chrome", "urllib")
READER_HEADERS = {"X-No-Cache": "true"}


def _get_headers():
    headers = dict(READER_HEADERS)
    api_key = os.environ.get("JINA_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key.strip()}"
    return headers


def _get_reader(url, transport="curl"):
    headers = _get_headers()
    max_retries = 3

    for attempt in range(max_retries):
        try:
            if transport == "urllib":
                import urllib.error
                import urllib.request
                request = urllib.request.Request(READER_PREFIX + url, headers=headers)
                try:
                    response = urllib.request.urlopen(request, timeout=READER_TIMEOUT)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    resp_obj = SimpleNamespace(
                        status_code=response.status,
                        text=response.read(4 * 1024 * 1024).decode("utf-8", errors="replace"),
                    )
            else:
                from curl_cffi import requests
                options = {"impersonate": "chrome"} if transport == "chrome" else {}
                resp_obj = requests.get(
                    READER_PREFIX + url,
                    timeout=READER_TIMEOUT,
                    headers=headers,
                    allow_redirects=False,
                    **options,
                )

            # Ritorna la risposta ottenuta
            return resp_obj

        except Exception as exc:
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            code = getattr(exc, "code", None)
            raise HttpScanError("reader_transport",
                                transport_error_code=code if isinstance(code, int) else None) from None

    raise HttpScanError("reader_transport")


def _parse_reader(response):
    if response.status_code != 200:
        error = HttpScanError("reader_http_status", response.status_code)
        try:
            payload = json.loads(response.text)
        except (ValueError, TypeError):
            payload = None
        if isinstance(payload, dict):
            error.details = {
                key: value for key, value in payload.items()
                if (key in ("code", "status") and isinstance(value, int))
                   or (key == "name" and isinstance(value, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", value))
            }
        raise error
    raw = response.text.split("Markdown Content:", 1)[-1].strip()
    if raw.startswith("```json\n") and raw.endswith("```"):
        raw = raw[len("```json\n"):-3].strip()
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        raise HttpScanError("reader_invalid_json", response.status_code) from None
    candidates = [payload]
    if isinstance(payload, dict):
        for container in (payload, payload.get("data")):
            content = container.get("content") if isinstance(container, dict) else None
            if not isinstance(content, str):
                continue
            content = content.split("Markdown Content:", 1)[-1].strip()
            if content.startswith("```json\n") and content.endswith("```"):
                content = content[len("```json\n"):-3].strip()
            try:
                candidates.append(json.loads(content))
            except (ValueError, TypeError):
                continue
    for candidate in candidates:
        if _captcha_object(candidate):
            raise HttpScanError("reader_captcha", response.status_code)
        try:
            parsed = parse_search_json(candidate)
        except Exception:
            raise HttpScanError("reader_malformed_results", response.status_code) from None
        if parsed is not None:
            return parsed
    error = HttpScanError("reader_unrecognized_results", response.status_code)
    if isinstance(payload, dict):
        known = {"code", "status", "data", "content", "title", "url", "results", "totalAds", "maxPages", "name",
                 "message"}
        error.details = {"root_keys": sorted(set(payload) & known)}
        if isinstance(payload.get("data"), dict):
            error.details["data_keys"] = sorted(set(payload["data"]) & known)
    raise error


def scan_reader(max_pages=10, transport="curl"):
    """Return all listings; raise a sanitized HttpScanError on any failure."""
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        raise HttpScanError("reader_invalid_page_limit")
    if transport not in READER_TRANSPORTS:
        raise HttpScanError("reader_invalid_transport")
    url = diagnostic_targets()[2][1]
    expected_total = None
    pending = {1}
    visited = set()
    by_id = {}
    while pending:
        if len(visited) >= max_pages:
            raise HttpScanError("reader_page_limit")
        number = min(pending)
        pending.remove(number)
        target = url if number == 1 else _page_url(url, number)
        page = _parse_reader(_get_reader(target, transport))
        total = page["total"]
        if expected_total is not None and total != expected_total:
            raise HttpScanError("reader_total_changed")
        expected_total = total
        visited.add(number)
        by_id.update((row["id"], row) for row in page["rows"])
        pages = page.get("maxPages")
        if pages is not None:
            if pages > max_pages:
                raise HttpScanError("reader_page_limit")
            pending.update(set(range(1, pages + 1)) - visited)
    if expected_total is None or len(by_id) != expected_total:
        raise HttpScanError("reader_incomplete_results")
    return list(by_id.values())
