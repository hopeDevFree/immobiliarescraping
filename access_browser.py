"""Bounded browser access experiments, with no scheduler or CAPTCHA solver.

Each variant owns a separate, reusable browser profile. Returned evidence contains
only selected headers and query-free network paths; cookies and challenge URLs
are never included.
"""

import asyncio
import json
import math
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

from bs4 import BeautifulSoup
import nodriver as nd
from nodriver import ProtocolException, cdp

from browser_runtime import open_browser
from listing_parser import inspect_html
from scraper import datadome_captcha, page_url
from search_config import RADIUS_URL


VARIANTS = ("direct_tab", "home_then_search", "plain_start")
HOME_URL = "https://www.immobiliare.it/"
MAX_PAGES = 10


class _ProbeFailure(Exception):
    def __init__(self, reason, blocked=False):
        self.reason = reason
        self.blocked = blocked


def _headers(headers, names):
    """Use an allowlist: raw request/response headers may contain credentials."""
    wanted = {name.lower() for name in names}
    return {
        str(key).lower(): str(value)[:512]
        for key, value in (headers or {}).items()
        if str(key).lower() in wanted
    }


def _network_path(url):
    parts = urlsplit(url)
    return {"host": parts.hostname, "path": parts.path[:180]}


def _blocked(info, html):
    if datadome_captcha(info):
        return "datadome_captcha"
    text = info.get("text_excerpt", "").lower()
    if "please enable js and disable any ad blocker" in text:
        return "datadome_challenge"
    soup = BeautifulSoup(html, "html.parser")
    for frame in soup.select("iframe[src]"):
        source = urlsplit(urljoin(HOME_URL, frame["src"]))
        host = source.hostname or ""
        if host == "captcha-delivery.com" or host.endswith(".captcha-delivery.com"):
            return "datadome_challenge_frame"
    return None


def _normalized_rows(info):
    result = []
    for row in info.get("rendered_listings", info.get("listings", [])):
        ident = row.get("id")
        if isinstance(ident, bool) or not str(ident).isdigit() or int(ident) <= 0:
            raise _ProbeFailure("invalid_listing_id")
        ident = int(ident)
        price = row.get("rent_eur")
        if price is not None:
            try:
                price = float(price)
            except (ValueError, TypeError):
                raise _ProbeFailure("invalid_listing_price") from None
            if not math.isfinite(price):
                raise _ProbeFailure("invalid_listing_price")
        result.append({
            "id": ident,
            "title": str(row.get("title") or "")[:500],
            "url": f"https://www.immobiliare.it/annunci/{ident}/",
            "rent_eur": price,
        })
    return result


def _linked_pages(html):
    pages = set()
    for link in BeautifulSoup(html, "html.parser").select("a[href]"):
        target = urlsplit(urljoin(RADIUS_URL, link["href"]))
        if target.hostname != "www.immobiliare.it" or target.path.rstrip("/") != "/search-list":
            continue
        for page in parse_qs(target.query).get("pag", []):
            if page.isdigit() and int(page) >= 1:
                pages.add(int(page))
    return pages


async def _html(tab):
    try:
        return await asyncio.wait_for(tab.evaluate("document.documentElement.outerHTML"), timeout=25)
    except ProtocolException as exc:
        if "Session with given id not found" not in str(exc):
            raise
        tab.session_id = None
        return await asyncio.wait_for(tab.evaluate("document.documentElement.outerHTML"), timeout=25)


async def _search_page(tab, url, evidence, navigate=True):
    if navigate:
        await tab.send(cdp.page.navigate(url))
    previous = None
    stable = 0
    for _ in range(40):
        await asyncio.sleep(1)
        html = await _html(tab)
        info = inspect_html(html, url)
        evidence["title"] = str(info.get("title") or "")[:240] or None
        reason = _blocked(info, html)
        if reason:
            raise _ProbeFailure(reason, blocked=True)
        rows = _normalized_rows(info)
        total = info.get("displayed_total_ads", info.get("total_ads"))
        ids = tuple(row["id"] for row in rows)
        if isinstance(total, int) and not isinstance(total, bool) and total >= 0 and (ids or total == 0):
            current = (ids, total)
            stable = stable + 1 if current == previous else 0
            if stable >= 2:
                return rows, total, _linked_pages(html)
            previous = current
    if any(item["status"] in (401, 403, 429) for item in evidence["network_failures"]):
        raise _ProbeFailure("access_denied", blocked=True)
    raise _ProbeFailure("results_not_loaded")


@asynccontextmanager
async def _plain_browser(executable, profile):
    """Open Chromium normally, then connect after its first navigation settles."""
    host = "127.0.0.1"
    port = nd.util.free_port()
    process = None
    browser = None
    try:
        process = await asyncio.create_subprocess_exec(
            executable,
            f"--user-data-dir={profile}",
            f"--remote-debugging-host={host}",
            f"--remote-debugging-port={port}",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--window-size=1366,900",
            RADIUS_URL,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        # No CDP commands are sent during initial page loading.
        await asyncio.sleep(12)
        if process.returncode is not None:
            raise _ProbeFailure("plain_browser_exited_before_attach")
        browser = await nd.start(
            host=host, port=port, browser_executable_path=executable,
            user_data_dir=str(profile),
        )
        yield browser
    finally:
        # nd.start can register a connection before raising. Only clean up the
        # connection to this exact localhost debugger, including that failure.
        registry = nd.util.get_registered_instances()
        connections = {
            instance for instance in tuple(registry)
            if getattr(getattr(instance, "config", None), "host", None) == host
            and getattr(getattr(instance, "config", None), "port", None) == port
        }
        if browser is not None:
            connections.add(browser)
        try:
            for connection in connections:
                try:
                    for target in tuple(connection.targets):
                        with suppress(Exception):
                            await asyncio.wait_for(target.aclose(), timeout=2)
                    with suppress(Exception):
                        await asyncio.wait_for(connection.aclose(), timeout=3)
                finally:
                    registry.discard(connection)
        finally:
            if process is not None:
                if process.returncode is None:
                    with suppress(ProcessLookupError):
                        process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=10)
                except TimeoutError:
                    with suppress(ProcessLookupError):
                        process.kill()
                    await asyncio.wait_for(process.wait(), timeout=5)


async def probe_browser(executable, profile_root, variant):
    """Read the exact search once; return complete listings or a sanitized failure.

    The overall budget is 120 seconds, including
    Chromium startup. This function does not start the service, repeat itself,
    or interact with challenges.
    """
    if variant not in VARIANTS:
        raise ValueError(f"Unknown browser variant: {variant}")
    started = time.monotonic()
    evidence = {"title": None, "documents": [], "network_failures": [], "browser": {}}
    result = {"variant": variant, "status": "failed", "total": None, "evidence": evidence}
    requests = {}
    documents = {}
    request_header_names = ("Accept", "Accept-Language", "Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site")

    def remember_request(event):
        # Even non-document requests may later be reported as an HTTP failure.
        if len(requests) >= 500:
            requests.pop(next(iter(requests)))
        ident = str(event.request_id)
        selected = _headers(event.request.headers, request_header_names)
        # ExtraInfo can arrive first and contains the actual network headers.
        selected.update(requests.get(ident, {}))
        requests[ident] = selected

    def remember_request_extra(event):
        ident = str(event.request_id)
        requests.setdefault(ident, {}).update(_headers(event.headers, request_header_names))
        if ident in documents:
            documents[ident]["request_headers"] = dict(requests[ident])

    def remember_response(event):
        response = event.response
        is_document = event.type_ == cdp.network.ResourceType.DOCUMENT
        target = urlsplit(response.url)
        if is_document:
            record = {
                **_network_path(response.url),
                "status": int(response.status),
                "mime_type": str(response.mime_type)[:120],
                "response_headers": _headers(response.headers, ("Content-Type",)),
                "request_headers": dict(requests.get(str(event.request_id), {})),
            }
            documents[str(event.request_id)] = record
            evidence["documents"].append(record)
            del evidence["documents"][:-20]
        if target.hostname == "www.immobiliare.it" and response.status >= 400 and event.type_ in (
            cdp.network.ResourceType.DOCUMENT, cdp.network.ResourceType.XHR, cdp.network.ResourceType.FETCH,
        ):
            evidence["network_failures"].append({**_network_path(response.url), "status": int(response.status)})
            del evidence["network_failures"][:-10]

    try:
        async with asyncio.timeout(120):
            profile = Path(profile_root) / variant
            profile.mkdir(parents=True, exist_ok=True)
            runtime = _plain_browser if variant == "plain_start" else open_browser
            async with runtime(executable, profile) as browser:
                info = browser.info or {}
                evidence["browser"] = {
                    key: str(info[key])[:512] for key in ("Browser", "User-Agent", "Protocol-Version", "V8-Version")
                    if key in info
                }
                if variant == "plain_start":
                    tabs = browser.tabs
                    if not tabs:
                        raise _ProbeFailure("no_browser_tab")
                    tab = next((item for item in tabs if urlsplit(item.target.url).hostname == "www.immobiliare.it"), tabs[0])
                    evidence["initial_navigation_before_network_observation"] = True
                else:
                    tab = await browser.get("about:blank")
                try:
                    # Plain-start intentionally loads before observation. Other
                    # variants register before their initial site navigation.
                    tab.add_handler(cdp.network.RequestWillBeSent, remember_request)
                    tab.add_handler(cdp.network.RequestWillBeSentExtraInfo, remember_request_extra)
                    tab.add_handler(cdp.network.ResponseReceived, remember_response)
                    await tab.send(cdp.network.enable())
                    await tab.send(cdp.page.enable())
                    environment = await asyncio.wait_for(tab.evaluate("JSON.stringify({"
                        "language:navigator.language,languages:navigator.languages,"
                        "timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,"
                        "webdriver:navigator.webdriver,platform:navigator.platform,"
                        "screen:{width:screen.width,height:screen.height,colorDepth:screen.colorDepth},"
                        "window:{width:innerWidth,height:innerHeight}})"), timeout=10)
                    evidence["environment"] = json.loads(environment)
                    if variant == "home_then_search":
                        await tab.send(cdp.page.navigate(HOME_URL))
                        await asyncio.sleep(7)
                        html = await _html(tab)
                        home = inspect_html(html, HOME_URL)
                        evidence["home_title"] = str(home.get("title") or "")[:240] or None
                        reason = _blocked(home, html)
                        if reason:
                            evidence["stage"] = "home"
                            raise _ProbeFailure(reason, blocked=True)
                    evidence["stage"] = "search"
                    rows, total, pages = await _search_page(tab, RADIUS_URL, evidence, navigate=variant != "plain_start")
                    by_id = {row["id"]: row for row in rows}
                    visited = {1}
                    pending = pages - visited
                    while pending:
                        if len(visited) >= MAX_PAGES:
                            raise _ProbeFailure("page_limit_exceeded")
                        number = min(pending)
                        pending.remove(number)
                        entries, page_total, more = await _search_page(tab, page_url(RADIUS_URL, number), evidence)
                        if page_total != total:
                            raise _ProbeFailure("search_changed_during_pagination")
                        by_id.update({row["id"]: row for row in entries})
                        visited.add(number)
                        pending.update(more - visited)
                    result["total"] = total
                    evidence["pages_read"] = sorted(visited)
                    evidence["extracted_count"] = len(by_id)
                    if len(by_id) != total:
                        raise _ProbeFailure("incomplete_search")
                    result.update(status="ok", listings=list(by_id.values()))
                finally:
                    with suppress(Exception):
                        await asyncio.wait_for(tab.close(), timeout=5)
    except _ProbeFailure as exc:
        result.update(status="blocked" if exc.blocked else "failed", reason=exc.reason)
    except TimeoutError:
        result.update(status="timeout", reason="browser_probe_budget_exceeded")
    except Exception as exc:
        # Exception text can include entire challenge URLs or browser profile data.
        result.update(status="failed", reason="browser_probe_error", error_type=type(exc).__name__)
    if result["status"] != "ok":
        result.pop("listings", None)
    result["elapsed_seconds"] = round(time.monotonic() - started, 2)
    return result
