"""Read the unchanged public search API through Jina Reader anonymously.

No account, API key, CAPTCHA interaction, cookie forwarding, or scheduler is
used. Reader caches are explicitly disabled for each request. A scan succeeds
only when every page has a stable total and all unique listing IDs are present.
"""

import json

from access_http import HttpScanError, _captcha_object, _page_url, diagnostic_targets, parse_search_json


READER_PREFIX = "https://r.jina.ai/"
READER_TIMEOUT = 30


def _get_reader(url):
    from curl_cffi import requests

    try:
        # Each GET owns a fresh session: no retained/forwarded cookies or auth.
        return requests.get(
            READER_PREFIX + url,
            timeout=READER_TIMEOUT,
            headers={"X-No-Cache": "true"},
            allow_redirects=False,
        )
    except Exception as exc:
        code = getattr(exc, "code", None)
        raise HttpScanError("reader_transport", transport_error_code=code if isinstance(code, int) else None) from None


def _parse_reader(response):
    if response.status_code != 200:
        raise HttpScanError("reader_http_status", response.status_code)
    raw = response.text.split("Markdown Content:", 1)[-1].strip()
    if raw.startswith("```json\n") and raw.endswith("```"):
        raw = raw[len("```json\n"):-3].strip()
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        raise HttpScanError("reader_invalid_json", response.status_code) from None
    if _captcha_object(payload):
        raise HttpScanError("reader_captcha", response.status_code)
    try:
        parsed = parse_search_json(payload)
    except Exception:
        raise HttpScanError("reader_malformed_results", response.status_code) from None
    if parsed is None:
        raise HttpScanError("reader_unrecognized_results", response.status_code)
    return parsed


def scan_reader(max_pages=10):
    """Return all listings; raise a sanitized HttpScanError on any failure."""
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        raise HttpScanError("reader_invalid_page_limit")
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
        page = _parse_reader(_get_reader(target))
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
