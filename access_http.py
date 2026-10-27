"""Bounded HTTP access diagnostics for the user's unchanged search.

Run with ``python access_http.py --probe`` (requires curl_cffi), or import
``parse_search_json`` or ``scan_http``. No browser, CAPTCHA solving or scheduler
is started. The diagnostic probe reads only page 1; scan_http validates every
page before returning listings. Requests have a 15 second timeout and no retries
or redirect following. Diagnostic output excludes cookies, headers, response
bodies and CAPTCHA URLs/tokens.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote_plus, urljoin, urlsplit, urlunsplit

from search_config import RADIUS_URL

API_PATHS = (
    "/api-next/search-list/listings/",
    "/api-next/search-list/real-estates/",
)
PROFILES = ("chrome", "safari")
REQUEST_TIMEOUT = 15
MAX_REQUESTS = 6


def diagnostic_urls():
    """Keep the exact original query string, including its order/encoding."""
    parsed = urlsplit(RADIUS_URL)
    return [RADIUS_URL] + [
        urlunsplit(parsed._replace(path=path)) for path in API_PATHS
    ]


def diagnostic_targets():
    """Six fixed probes, including one explicit routing-parameter variant."""
    html, listings, estates = diagnostic_urls()
    routed = listings + "&path=%2Fsearch-list%2F"
    return (
        ("chrome", html),
        ("chrome", listings),
        ("chrome", routed),
        ("chrome", estates),
        ("safari", html),
        ("safari", listings),
    )


class HttpScanError(RuntimeError):
    """Only fixed categories and numeric metadata are exposed to callers."""

    def __init__(self, category, status=None, transport_error_code=None):
        self.category = category
        self.status = status
        self.transport_error_code = transport_error_code
        message = f"HTTP_SCAN_FAILED category={category}"
        if isinstance(status, int):
            message += f" status={status}"
        if isinstance(transport_error_code, int):
            message += f" transport_error_code={int(transport_error_code)}"
        super().__init__(message)


def _new_session(profile):
    from curl_cffi import requests

    return requests.Session(impersonate=profile)


def _page_url(url, number):
    """Change only pag, preserving all other original query bytes."""
    parsed = urlsplit(url)
    parts = parsed.query.split("&") if parsed.query else []
    matches = [index for index, part in enumerate(parts) if unquote_plus(part.split("=", 1)[0]) == "pag"]
    if len(matches) > 1:
        raise HttpScanError("ambiguous_page_parameter")
    if matches:
        index = matches[0]
        parts[index] = parts[index].split("=", 1)[0] + "=" + str(number)
    else:
        parts.append("pag=" + str(number))
    return urlunsplit(parsed._replace(query="&".join(parts)))


def _captcha_object(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("url"), str):
        return False
    target = urlsplit(payload["url"])
    host = target.hostname or ""
    return (host == "captcha-delivery.com" or host.endswith(".captcha-delivery.com")) and target.path.startswith(
        "/captcha/")


def _integer(value):
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
        return number if number >= 0 and str(number) == str(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _price(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) and number >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def parse_search_json(payload):
    """Return normalized real results and page metadata, or None.

    Known direct API and Next.js dehydrated result objects are accepted. A
    challenge object, a page shell, or unrelated JSON never becomes an empty
    successful search. An explicitly empty ``results`` list and totalAds=0
    represent a genuine empty result set.
    """
    candidates = [payload]
    if isinstance(payload, dict):
        props = payload.get("props", {})
        page_props = props.get("pageProps", {}) if isinstance(props, dict) else {}
        for container in (payload, page_props):
            if not isinstance(container, dict):
                continue
            if isinstance(container.get("data"), dict):
                candidates.append(container["data"])
            state = container.get("dehydratedState", {})
            if isinstance(state, dict):
                for query in state.get("queries", []):
                    if not isinstance(query, dict):
                        continue
                    key = query.get("queryKey", [])
                    if not isinstance(key, list) or not key or key[0] != "real-estate-list":
                        continue
                    query_state = query.get("state", {})
                    if isinstance(query_state, dict):
                        candidates.append(query_state.get("data"))
    for candidate in candidates:
        if not isinstance(candidate, dict) or not isinstance(candidate.get("results"), list):
            continue
        total = _integer(candidate.get("totalAds"))
        pages = _integer(candidate.get("maxPages"))
        rows = []
        malformed = False
        for item in candidate["results"]:
            estate = item.get("realEstate") if isinstance(item, dict) else None
            if not isinstance(estate, dict) or not _integer(estate.get("id")):
                malformed = True
                break
            identity = _integer(estate["id"])
            seo = item.get("seo", {}) if isinstance(item.get("seo"), dict) else {}
            url = seo.get("url")
            price = estate.get("price", {}) if isinstance(estate.get("price"), dict) else {}

            # Estrazione caratteristiche e descrizione dal payload JSON
            properties = estate.get("properties", []) if isinstance(estate.get("properties"), list) else []
            prop = properties[0] if properties and isinstance(properties[0], dict) else {}
            description = prop.get("description") or prop.get("caption") or seo.get("description")

            rows.append({
                "id": identity,
                "title": estate.get("title") if isinstance(estate.get("title"), str) else None,
                "url": urljoin(RADIUS_URL, url) if isinstance(url,
                                                              str) else f"https://www.immobiliare.it/annunci/{identity}/",
                "rent_eur": _price(price.get("value")),
                "description": description if isinstance(description, str) else None,
                "surface": prop.get("surface"),
                "rooms": prop.get("rooms"),
            })
        if not malformed and total is not None and (rows or total == 0):
            if total < len(rows) or (total == 0 and rows):
                continue
            return {"rows": rows, "total": total, "maxPages": pages}
    return None


def _decode_response(response, url):
    """Read known search schemas and same-path pagination links."""
    title = None
    json_type = None
    pages = set()
    try:
        payload = response.json()
    except (ValueError, TypeError):
        payload = None
    if payload is not None:
        json_type = type(payload).__name__
        if _captcha_object(payload):
            raise HttpScanError("captcha", response.status_code)
        parsed = parse_search_json(payload)
    else:
        from bs4 import BeautifulSoup
        from listing_parser import inspect_html

        soup = BeautifulSoup(response.text, "html.parser")
        if soup.title:
            title = soup.title.get_text(" ", strip=True)[:160]
        text = soup.get_text(" ", strip=True)
        try:
            embedded_captcha = _captcha_object(json.loads(text))
        except (ValueError, TypeError):
            embedded_captcha = False
        if embedded_captcha or "please enable js and disable any ad blocker" in text.lower():
            raise HttpScanError("captcha", response.status_code)
        script = soup.find("script", id="__NEXT_DATA__")
        parsed = None
        if script:
            try:
                parsed = parse_search_json(json.loads(script.string or script.get_text()))
            except (ValueError, TypeError):
                pass
        if parsed is None:
            info = inspect_html(response.text, url)
            rows = info.get("rendered_listings")
            total = info.get("displayed_total_ads")
            if total is not None and (rows is not None or total == 0):
                parsed = {"rows": rows or [], "total": total, "maxPages": None}
        source = urlsplit(url)
        for link in soup.select("a[href]"):
            target = urlsplit(urljoin(url, link["href"]))
            if target.hostname != source.hostname or target.path.rstrip("/") != source.path.rstrip("/"):
                continue
            for page in parse_qs(target.query).get("pag", []):
                number = _integer(page)
                if number:
                    pages.add(number)
    if response.status_code != 200:
        raise HttpScanError("blocked" if response.status_code in (401, 403, 429) else "http_status",
                            response.status_code)
    if parsed is None:
        raise HttpScanError("unrecognized_results", response.status_code)
    return parsed, pages, title, json_type


def summarize_response(response, url=RADIUS_URL):
    """Produce diagnostic metadata without leaking the response body."""
    result = {
        "status": response.status_code,
        "title": None,
        "json_type": None,
        "recognized_results": False,
        "count": None,
        "total": None,
        "maxPages": None,
        "ids": [],
    }
    try:
        parsed, _, title, json_type = _decode_response(response, url)
        result.update({
            "title": title,
            "json_type": json_type,
            "recognized_results": True,
            "count": len(parsed["rows"]),
            "total": parsed["total"],
            "maxPages": parsed["maxPages"],
            "ids": [row["id"] for row in parsed["rows"]],
        })
    except HttpScanError as exc:
        result["category"] = exc.category
        # Record just the data type, never the contents of a CAPTCHA object.
        try:
            result["json_type"] = type(response.json()).__name__
        except (ValueError, TypeError):
            pass
    return result


def _get(session, url):
    try:
        return session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False)
    except Exception as exc:
        code = getattr(exc, "code", None)
        raise HttpScanError("transport", transport_error_code=code if isinstance(code, int) else None) from None


def scan_http(profile, url=RADIUS_URL, max_pages=10):
    """Return every listing only after complete, stable-total validation.

    This synchronous function can run with ``await asyncio.to_thread(scan_http,
    profile, url)``. The original URL is requested verbatim; subsequent pages
    change only ``pag``. The same in-memory session handles all pages. No URL,
    body, cookie, CAPTCHA identifier or transport exception text enters errors.
    """
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        raise HttpScanError("invalid_page_limit")
    try:
        session = _new_session(profile)
    except Exception:
        raise HttpScanError("transport_setup") from None
    with session:
        by_id = {}
        expected_total = None
        visited = set()
        pending = {1}
        while pending:
            if len(visited) >= max_pages:
                raise HttpScanError("page_limit")
            number = min(pending)
            pending.remove(number)
            target = url if number == 1 else _page_url(url, number)
            response = _get(session, target)
            try:
                page, linked, _, _ = _decode_response(response, target)
            except HttpScanError:
                raise
            except Exception:
                raise HttpScanError("malformed_results", response.status_code) from None
            total = page["total"]
            if expected_total is not None and total != expected_total:
                raise HttpScanError("total_changed", response.status_code)
            expected_total = total
            visited.add(number)
            for row in page["rows"]:
                by_id[row["id"]] = row
            advertised_pages = page.get("maxPages")
            if advertised_pages is not None:
                if advertised_pages > max_pages:
                    raise HttpScanError("page_limit", response.status_code)
                linked.update(range(1, advertised_pages + 1))
            pending.update(linked - visited)
        if expected_total is None or len(by_id) != expected_total:
            raise HttpScanError("incomplete_results")
        return list(by_id.values())


def probe_http():
    """Execute a fixed six-request matrix without retries or redirects."""
    targets = diagnostic_targets()
    original_query = urlsplit(RADIUS_URL).query
    assert len(targets) <= MAX_REQUESTS
    assert all(urlsplit(url).query.removesuffix("&path=%2Fsearch-list%2F") == original_query for _, url in targets)
    results = []
    for profile in PROFILES:
        # One in-memory session per client; no stored profile or cookie export.
        with _new_session(profile) as session:
            for _, url in (item for item in targets if item[0] == profile):
                routed = urlsplit(url).query != original_query
                entry = {
                    "profile": profile, "url": url,
                    "query_unchanged": not routed, "filters_unchanged": True,
                    "technical_parameter_added": "path=/search-list/" if routed else None,
                }
                try:
                    response = session.get(
                        url, timeout=REQUEST_TIMEOUT, allow_redirects=False,
                    )
                    entry.update(summarize_response(response, url))
                except Exception as exc:
                    # Some transport exception messages embed response text.
                    entry.update({"error_type": type(exc).__name__})
                    code = getattr(exc, "code", None)
                    if isinstance(code, int):
                        entry["transport_error_code"] = int(code)
                results.append(entry)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_url": RADIUS_URL,
        "filters_unchanged": True,
        "requests": len(results),
        "timeout_seconds": REQUEST_TIMEOUT,
        "results": results,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", action="store_true", help="Make exactly six bounded GET requests")
    args = parser.parse_args()
    if args.probe:
        print(json.dumps(probe_http(), ensure_ascii=False, indent=2))
    else:
        parser.print_help()
