"""Compare the user's search URL with the configured search in one browser."""

import argparse
import asyncio
import json
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import nodriver as nd
from bs4 import BeautifulSoup

from probe_access import REPORTS, inspect_html
from search_config import DEFAULT_SEARCH, SEARCHES

USER_URL = "https://www.immobiliare.it/search-list/?idContratto=2&idCategoria=1&arredato=on&__lang=it&raggio=5000&centro=40.87414%2C14.34105&pag=1"


def with_params(url, **updates):
    parts = urlsplit(url)
    params = parse_qs(parts.query)
    params.update({k: [str(v)] for k, v in updates.items()})
    return urlunsplit(parts._replace(query=urlencode(params, doseq=True)))


async def read_page(browser, url, label):
    tab = await browser.get(url, new_tab=True)
    try:
        previous = None
        stable = 0
        for _ in range(60):
            await asyncio.sleep(1)
            html = await asyncio.wait_for(tab.get_content(), timeout=15)
            info = inspect_html(html, url)
            rows = info.get("rendered_listings", info.get("listings", []))
            ids = tuple(row["id"] for row in rows)
            stable = stable + 1 if ids and ids == previous else 0
            previous = ids
            if stable >= 3 or info.get("total_ads") == 0:
                break
        else:
            raise TimeoutError("Search cards did not finish loading")
        final_url = await tab.evaluate("location.href")
        REPORTS.joinpath(f"compare-{label}.html").write_text(html, encoding="utf-8")
        await tab.save_screenshot(str(REPORTS / f"compare-{label}.png"))
        soup = BeautifulSoup(html, "html.parser")
        pages = sorted({
            int(p) for a in soup.select("a[href]")
            for p in parse_qs(urlsplit(a["href"]).query).get("pag", []) if p.isdigit()
        })
        return {
            "requested_url": url, "final_url": final_url,
            "displayed_total_ads": info.get("displayed_total_ads", info.get("total_ads")),
            "listing_count_on_page": len(rows), "listings": rows,
            "linked_page_numbers": pages,
        }
    finally:
        await tab.close()


async def complete_pagination(browser, entry, label):
    if "listings" not in entry:
        return entry
    pages = [{"page": 1, "listing_count": len(entry["listings"])}]
    rows = {row["id"]: row for row in entry["listings"]}
    pending = set(entry.get("linked_page_numbers", [])) - {1}
    visited = {1}
    while pending:
        number = min(pending)
        pending.remove(number)
        if number in visited:
            continue
        if len(visited) >= 10:
            raise ValueError("Comparison limited to ten pages per search")
        page = await asyncio.wait_for(read_page(
            browser, with_params(entry["requested_url"], pag=number), f"{label}-page-{number}"
        ), timeout=100)
        visited.add(number)
        pages.append({"page": number, "listing_count": len(page["listings"])})
        rows.update({row["id"]: row for row in page["listings"]})
        pending.update(set(page.get("linked_page_numbers", [])) - visited)
    entry["pages"] = pages
    entry["listings"] = list(rows.values())
    entry["unique_listing_count"] = len(rows)
    entry["count_matches_displayed_total"] = len(rows) == entry.get("displayed_total_ads")
    return entry


def compare_ids(searches):
    ids = {k: {r["id"] for r in v["listings"]} for k, v in searches.items() if "listings" in v}
    return {
        "user_only": sorted(ids["user"] - ids["code"]),
        "code_only": sorted(ids["code"] - ids["user"]),
        "common": sorted(ids["code"] & ids["user"]),
        "same_ids": ids["user"] == ids["code"],
        "same_order": [r["id"] for r in searches["user"]["listings"]] == [r["id"] for r in searches["code"]["listings"]],
        "same_prices": {r["id"]: r["rent_eur"] for r in searches["user"]["listings"]} == {r["id"]: r["rent_eur"] for r in searches["code"]["listings"]},
        "same_requested_url": searches["user"]["requested_url"] == searches["code"]["requested_url"],
        "complete": all(v.get("count_matches_displayed_total") for v in searches.values()),
    }


async def compare(executable):
    REPORTS.mkdir(exist_ok=True)
    result = {"checked_at_utc": datetime.now(timezone.utc).isoformat(), "searches": {}}
    searches = {
        "user": USER_URL,
        "code": SEARCHES[DEFAULT_SEARCH],
    }
    if searches["user"] != searches["code"]:
        raise ValueError("Configured URL must be byte-for-byte identical to the user's link")
    browser = await nd.start(
        browser_executable_path=executable, headless=False,
        user_data_dir=str(REPORTS / "browser-profile"),
    )
    try:
        for label, url in searches.items():
            try:
                entry = await asyncio.wait_for(read_page(browser, url, f"exact-{label}"), timeout=100)
                entry = await complete_pagination(browser, entry, f"exact-{label}")
            except Exception as exc:
                entry = {"requested_url": url, "error": f"{type(exc).__name__}: {exc}"}
            result["searches"][label] = entry
            print(json.dumps({"search": label, **{k: v for k, v in entry.items() if k != "listings"}}, ensure_ascii=True), flush=True)
        if all("listings" in e for e in result["searches"].values()):
            result["comparison"] = compare_ids(result["searches"])
            print(json.dumps(result["comparison"], ensure_ascii=True), flush=True)
    finally:
        await browser.aclose()
        REPORTS.joinpath("search-comparison-exact.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    summary = result.get("comparison", {})
    return int(not all(summary.get(k) for k in ("same_ids", "same_prices", "same_requested_url", "complete")))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", required=True)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(compare(args.browser)))
