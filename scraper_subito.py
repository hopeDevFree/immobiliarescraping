import json
import logging
import os
import re
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

SUBITO_URL = "https://www.subito.it/annunci-locali/affitto/appartamenti/?rad=5000&lat=40.87414&lon=14.34105&fr=true"
# Offset per non sovrascrivere gli ID numerici di Immobiliare su Supabase
SUBITO_ID_OFFSET = 2_000_000_000


def fetch_subito_listings(only_privates=False):
    jina_url = f"https://r.jina.ai/{SUBITO_URL}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-No-Cache": "true",
        "X-Return-Format": "html",
    }

    # Se hai impostato la chiave JINA_API_KEY su Render, la usa
    jina_api_key = os.environ.get("JINA_API_KEY")
    if jina_api_key:
        headers["Authorization"] = f"Bearer {jina_api_key}"

    req = urllib.request.Request(jina_url, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            html = response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        logger.warning(f"Subito fetch failed (Jina HTTP {e.code}): {e.reason}")
        return []
    except Exception as e:
        logger.warning(f"Subito fetch failed with unexpected error: {e}")
        return []

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
    if not match:
        return []

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []

    items_state = data.get("props", {}).get("pageProps", {}).get("initialState", {}).get("items", {})
    ads = items_state.get("originalList") or items_state.get("list") or []

    rows = []
    for ad in ads:
        advertiser_type = ad.get("advertiser", {}).get("type")
        is_company = (advertiser_type == 1)

        # Se vuoi solo privati, scarta le agenzie
        if only_privates and is_company:
            continue

        # ID univoco ricavato dall'URN (es. urn:ad:665724802)
        raw_id = ad.get("urn", "").split(":")[-1]
        if not raw_id.isdigit():
            continue
        listing_id = SUBITO_ID_OFFSET + int(raw_id)

        # Prezzo e caratteristiche
        features = ad.get("features", {})
        price_val = features.get("/price", {}).get("values", [{}])[0].get("key")
        price = int(price_val) if price_val and str(price_val).isdigit() else None

        surface = features.get("/size", {}).get("values", [{}])[0].get("key")
        rooms = features.get("/rooms", {}).get("values", [{}])[0].get("key")

        tag = "🏢 [Agenzia]" if is_company else "👤 [Privato]"
        title = f"{tag} {ad.get('subject', 'Annuncio Subito')}"

        rows.append({
            "id": listing_id,
            "title": title,
            "price": price,
            "rent_eur": price,
            "surface": surface,
            "rooms": rooms,
            "url": ad.get("urls", {}).get("default"),
            "source": "subito",
        })

    return rows