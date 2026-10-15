"""Extract listings from initial HTML and rendered search cards."""

import json
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from search_config import distance_from_volla


def inspect_html(html, url):
    soup = BeautifulSoup(html, "html.parser")
    text = " ".join(soup.get_text(" ", strip=True).split())
    urls = {
        urljoin(url, a["href"]).split("?")[0]
        for a in soup.select("a[href]")
        if re.search(r"/annunci/\d+/?", a["href"])
    }
    data = soup.find("script", id="__NEXT_DATA__")
    result = {
        "title": soup.title.get_text() if soup.title else None,
        "listing_links": sorted(urls),
        "has_next_data": data is not None,
        "datadome_reference": "datadome" in html.lower() or "captcha-delivery.com" in html.lower(),
        "text_excerpt": text[:12000],
    }
    # Map/circle searches load cards client-side; __NEXT_DATA__ stays unchanged.
    cards = soup.select('[data-cy="listing-search-results"] li[id]')
    if cards:
        result["rendered_listings"] = []
        for card in cards:
            link = card.find("a", href=re.compile(r"/annunci/\d+/"))
            if not link or not card.get("id", "").isdigit():
                continue
            card_text = card.get_text(" ", strip=True)
            price = re.search(r"\u20ac\s*([\d.]+(?:,\d+)?)\s*/mese", card_text)
            result["rendered_listings"].append({
                "id": int(card["id"]),
                "title": link.get("title") or link.get_text(" ", strip=True),
                "url": urljoin(url, link["href"]).split("?")[0],
                "rent_eur": float(price[1].replace(".", "").replace(",", ".")) if price else None,
            })
    count = re.search(r"(\d[\d.]*)\s+risultat[oi]\b", text, re.IGNORECASE)
    if count:
        result["displayed_total_ads"] = int(count[1].replace(".", ""))
    if data:
        try:
            page = json.loads(data.string or data.get_text())
            props = page["props"]["pageProps"]
            estate = props.get("detailData", {}).get("realEstate")
            if isinstance(estate, dict):
                properties = estate.get("properties", [])
                prop = properties[0] if properties else {}
                result["detail"] = {
                    "id": estate.get("id"), "title": estate.get("title"),
                    "rent_eur": estate.get("price", {}).get("value"),
                    "description": prop.get("description"),
                    "surface": prop.get("surface"), "rooms": prop.get("rooms"),
                    "bedrooms": prop.get("bedRoomsNumber"),
                    "floor": prop.get("floor", {}).get("value"),
                    "features": prop.get("ga4features", []),
                    "condominium_expenses": prop.get("costs", {}).get("condominiumExpenses"),
                    "deposit": prop.get("rent", {}).get("deposit"),
                    "last_update": prop.get("lastUpdate"),
                    "total_monthly_cost_eur": None,
                    "furniture_inventory": None,
                    **distance_from_volla(prop.get("location") or {}),
                }
            queries = props.get("dehydratedState", {}).get("queries", [])
            for query in queries:
                key = query.get("queryKey", [])
                value = query.get("state", {}).get("data")
                if not key or key[0] != "real-estate-list" or not isinstance(value, dict):
                    continue
                result["result_count"] = value.get("count")
                result["total_ads"] = value.get("totalAds")
                result["max_pages"] = value.get("maxPages")
                result["listings"] = []
                for item in value.get("results", []):
                    estate = item.get("realEstate", {})
                    properties = estate.get("properties", [])
                    prop = properties[0] if properties else {}
                    result["listings"].append({
                        "id": estate.get("id"), "title": estate.get("title"),
                        "url": item.get("seo", {}).get("url"),
                        "rent_eur": estate.get("price", {}).get("value"),
                        "surface": prop.get("surface"), "rooms": prop.get("rooms"),
                        "floor": prop.get("floor", {}).get("value"),
                        "features": prop.get("ga4features", []),
                        "total_monthly_cost_eur": None,
                        "furniture_inventory": None,
                        **distance_from_volla(prop.get("location") or {}),
                    })
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            result["data_error"] = str(exc)
    return result
