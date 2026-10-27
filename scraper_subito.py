import json
import re
import urllib.request

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

    req = urllib.request.Request(jina_url, headers=headers)
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8")

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
    if not match:
        return []

    data = json.loads(match.group(1))
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