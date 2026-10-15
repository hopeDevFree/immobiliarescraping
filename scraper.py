"""Read every page of the exact configured search, without local filtering."""

import asyncio
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import nodriver as nd
from bs4 import BeautifulSoup

from listing_parser import inspect_html
from search_config import RADIUS_URL


def page_url(url, number):
    parts = urlsplit(url)
    params = parse_qs(parts.query)
    params["pag"] = [str(number)]
    return urlunsplit(parts._replace(query=urlencode(params, doseq=True)))


async def read_page(browser, url):
    tab = await browser.get(url, new_tab=True)
    try:
        previous = None
        stable = 0
        for _ in range(60):
            await asyncio.sleep(1)
            html = await asyncio.wait_for(tab.get_content(), timeout=10)
            info = inspect_html(html, url)
            rows = info.get("rendered_listings", info.get("listings", []))
            ids = tuple(row["id"] for row in rows)
            total = info.get("displayed_total_ads", info.get("total_ads"))
            if total is not None and (ids or total == 0):
                stable = stable + 1 if ids == previous else 0
                if stable >= 3:
                    break
            previous = ids
        else:
            raise RuntimeError("Search not readable: blocked, incomplete, or layout changed")
        soup = BeautifulSoup(html, "html.parser")
        pages = {
            int(p) for a in soup.select("a[href]")
            for p in parse_qs(urlsplit(a["href"]).query).get("pag", []) if p.isdigit()
        }
        return rows, total, pages
    finally:
        await tab.close()


async def collect_pages(browser, url=RADIUS_URL, max_pages=10):
    rows, total, linked = await read_page(browser, url)
    by_id = {row["id"]: row for row in rows}
    visited = {1}
    pending = linked - visited
    while pending:
        number = min(pending)
        pending.remove(number)
        if len(visited) >= max_pages:
            raise RuntimeError("Page limit exceeded; scan not saved as complete")
        entries, page_total, more = await read_page(browser, page_url(url, number))
        if page_total != total:
            raise RuntimeError("Search changed during pagination; retry on the next scheduled run")
        visited.add(number)
        by_id.update({row["id"]: row for row in entries})
        pending.update(more - visited)
    if total is None or len(by_id) != total:
        raise RuntimeError(f"Incomplete search: extracted {len(by_id)}, expected {total}")
    return list(by_id.values())


async def scan(executable, profile_dir, timeout=120):
    browser = None
    try:
        async with asyncio.timeout(timeout):
            browser = await nd.start(
                browser_executable_path=executable,
                user_data_dir=str(profile_dir), headless=False,
                sandbox=False,
                browser_args=["--disable-dev-shm-usage"],
            )
            return await collect_pages(browser)
    finally:
        if browser is not None:
            try:
                await asyncio.wait_for(browser.aclose(), timeout=10)
            except Exception:
                browser.stop()
