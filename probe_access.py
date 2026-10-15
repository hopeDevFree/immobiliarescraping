"""Small access test, independent of the upstream sales parser."""

import argparse
import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from listing_parser import inspect_html
from search_config import DEFAULT_SEARCH, RADIUS_METERS, SEARCHES, VOLLA_CENTER

ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "reports"


async def browser_html(url, executable, manual_seconds):
    import nodriver as nd

    browser = await nd.start(
        browser_executable_path=executable,
        user_data_dir=str(REPORTS / "browser-profile"),
        headless=False,
    )
    try:
        tab = await browser.get(url)
        for second in range(max(10, manual_seconds)):
            await asyncio.sleep(1)
            if second < 4:
                continue
            html = await tab.get_content()
            info = inspect_html(html, url)
            detail_match = re.search(r"/annunci/(\d+)/", url)
            if (detail_match and str(info.get("detail", {}).get("id")) == detail_match[1]) or (
                not detail_match and (info.get("listings") or info["listing_links"] or info.get("total_ads") == 0)
            ):
                break
        await tab.save_screenshot(str(REPORTS / "nodriver-page.png"))
        return await tab.get_content(), await tab.evaluate("location.href")
    finally:
        await browser.aclose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=["curl", "nodriver"], default="curl")
    parser.add_argument("--area", choices=["all", *SEARCHES], default=DEFAULT_SEARCH)
    parser.add_argument("--browser")
    parser.add_argument("--listing-id", type=int, help="Read one detail instead of its area's search")
    parser.add_argument("--manual-seconds", type=int, choices=range(10, 121), default=30)
    args = parser.parse_args()
    if args.listing_id and args.area == "all":
        parser.error("Use a single --area with --listing-id")
    if args.transport == "nodriver" and not args.browser:
        parser.error("--browser must point to a Chrome/Chromium executable")
    REPORTS.mkdir(exist_ok=True)
    searches = SEARCHES if args.area == "all" else {args.area: SEARCHES[args.area]}
    results = []
    for area, url in searches.items():
        label = f"detail-{args.listing_id}" if args.listing_id else area
        if args.listing_id:
            url = f"https://www.immobiliare.it/annunci/{args.listing_id}/"
        report = {"area": area, "url": url, "transport": args.transport}
        try:
            if args.transport == "curl":
                from curl_cffi import requests

                response = requests.get(url, impersonate="chrome", timeout=25)
                html, final_url = response.text, str(response.url)
                report["http_status"] = response.status_code
            else:
                html, final_url = asyncio.run(asyncio.wait_for(
                    browser_html(url, args.browser, args.manual_seconds),
                    timeout=args.manual_seconds + 90,
                ))
            report["final_url"] = final_url
            report.update(inspect_html(html, final_url))
            (REPORTS / f"{args.transport}-{label}.html").write_text(html, encoding="utf-8")
            report["status"] = (
                "links_found_unverified" if report["listing_links"]
                and report.get("http_status", 200) == 200 else "access_unverified"
            )
            if report.get("http_status", 200) == 200:
                if report.get("total_ads") == 0:
                    report["status"] = "empty_search_confirmed"
                elif args.listing_id:
                    report["status"] = (
                        "detail_loaded_unverified"
                        if report.get("detail", {}).get("id") == args.listing_id
                        else "access_unverified"
                    )
        except Exception as exc:
            report.update(status="error", error=f"{type(exc).__name__}: {exc}")
        results.append(report)
        print(json.dumps({k: v for k, v in report.items() if k != "text_excerpt"}, ensure_ascii=True), flush=True)
    suffix = f"-detail-{args.listing_id}" if args.listing_id else ""
    output = REPORTS / f"{args.transport}-access{suffix}.json"
    output.write_text(json.dumps({
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "distance_reference": {"center": VOLLA_CENTER, "radius_meters": RADIUS_METERS, "method": "great_circle", "note": "Distance to published pin; only_area markers are approximate"},
        "results": results,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return int(any(r["status"] == "error" for r in results))


if __name__ == "__main__":
    raise SystemExit(main())
