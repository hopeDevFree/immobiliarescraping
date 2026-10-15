"""Bounded compatibility probe for the unmodified Stemanz scraper. No Telegram I/O."""

import argparse
import importlib.util
import json
import math
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from search_config import DEFAULT_SEARCH, SEARCHES

ROOT = Path(__file__).resolve().parent


def fetch(url, report):
    # Match upstream transport, adding a timeout and explicit status checks.
    response = requests.get(url, allow_redirects=False, timeout=20)
    body = response.text.lower()
    entry = {"url": url, "http_status": response.status_code}
    if "datadome" in body or "captcha-delivery.com" in body:
        entry["protection"] = "DataDome"
    report["requests"].append(entry)
    response.raise_for_status()
    if response.is_redirect:
        raise ValueError("Redirect: upstream does not follow it; URL needs verification")
    if entry.get("protection"):
        raise ValueError("Protection page received instead of search results")
    return response.content


def run_probe(scraper_class, area, url, max_details):
    report = {"area": area, "url": url, "requests": [], "status": "error"}
    try:
        content = fetch(url, report)
        scraper = scraper_class(
            url, browse_all_pages=False, min_house_cost=0, wait=0, verbose=False
        )
        # Use the original link extractor against exactly the fetched page.
        scraper._get_page = lambda _: BytesIO(content)
        scraper.get_all_urls()
        urls = list(dict.fromkeys(scraper.urls_))
        report["listing_urls"] = urls
        if not urls:
            report["status"] = "no_links_unverified"
            report["note"] = "No links extracted; this does not prove there are no homes"
            return report

        def get_text(detail_url):
            html = fetch(detail_url, report)
            return " ".join(BeautifulSoup(html, "html.parser").get_text(" ").split())

        scraper._get_text = get_text
        report["sample"] = []
        for detail_url in urls[:max_details]:
            values = scraper._get_data(detail_url)._asdict()
            report["sample"].append({
                key: None if isinstance(value, float) and not math.isfinite(value) else value
                for key, value in values.items()
            })
        # The original parser is for sales: parsed values require manual validation.
        report["status"] = "links_found_values_unverified"
    except (requests.RequestException, ValueError, AttributeError, TypeError) as exc:
        report["error"] = str(exc)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--area", choices=["all", *SEARCHES], default=DEFAULT_SEARCH)
    parser.add_argument("--max-details", type=int, choices=range(0, 4), default=1)
    args = parser.parse_args()
    source = ROOT / "vendor/immobiscraper/immobiscraper/immobiscraper.py"
    if not source.is_file():
        parser.error("Clone https://github.com/Stemanz/immobiscraper into vendor/immobiscraper first")
    spec = importlib.util.spec_from_file_location("upstream_immobiscraper", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    searches = SEARCHES if args.area == "all" else {args.area: SEARCHES[args.area]}
    reports = []
    for area, url in searches.items():
        report = run_probe(module.Immobiliare, area, url, args.max_details)
        reports.append(report)
        statuses = [str(r["http_status"]) for r in report["requests"]]
        print(f"{area}: {report['status']} | HTTP {', '.join(statuses) or 'unavailable'}", flush=True)
    output = ROOT / "reports/immobiscraper-probe.json"
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps({
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "First page only; filters are those of each request URL; bills and fees unverified",
        "results": reports,
    }, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(f"Report: {output}")
    return 1 if any(r["status"] == "error" for r in reports) else 0


if __name__ == "__main__":
    raise SystemExit(main())
