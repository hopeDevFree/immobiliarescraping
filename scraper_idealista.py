import json
import logging
import math
import os
import re
import urllib.error
import urllib.request
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# URL di ricerca su Idealista (Sant'Anastasia e comuni confinanti)
IDEALISTA_URL = os.environ.get(
    "IDEALISTA_URL",
    "https://www.idealista.it/affitto-case/sant-anastasia-napoli,pollena-trocchia-napoli,somma-vesuviana-napoli,pomigliano-d-arco-napoli,cercola-napoli/"
)

# Coordinate del tuo punto centrale (Sant'Anastasia)
CENTER_LAT = 40.87414
CENTER_LON = 14.34105
MAX_DISTANCE_METERS = 5000  # Raggio esatto di 5 km

# Offset per non sovrascrivere gli ID di Immobiliare e Subito su Supabase
IDEALISTA_ID_OFFSET = 3_000_000_000

# Coordinate di riferimento dei centri comunali per gli annunci che non hanno GPS esatto
MUNICIPALITY_COORDS = {
    "sant'anastasia": (40.8741, 14.3411),
    "pollena trocchia": (40.8578, 14.3828),
    "somma vesuviana": (40.8722, 14.4372),
    "pomigliano d'arco": (40.9133, 14.3917),
    "cercola": (40.8594, 14.3547),
    "massa di somma": (40.8492, 14.3725),
    "san sebastiano al vesuvio": (40.8419, 14.3644),
}


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcola la distanza esatta in metri tra due coordinate GPS."""
    R = 6371000  # Raggio della Terra in metri
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def fetch_idealista_listings():
    jina_url = f"https://r.jina.ai/{IDEALISTA_URL}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-No-Cache": "true",
        "X-Return-Format": "html",
    }

    jina_api_key = os.environ.get("JINA_API_KEY")
    if jina_api_key:
        headers["Authorization"] = f"Bearer {jina_api_key}"

    req = urllib.request.Request(jina_url, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            html = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        logger.warning(f"Idealista fetch failed (HTTP {e.code}): {e.reason}")
        return []
    except Exception as e:
        logger.warning(f"Idealista fetch failed with error: {e}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    articles = soup.select("article.item[data-adid]") or soup.select("article.item")

    rows = []
    for art in articles:
        try:
            # 1. ID annuncio
            raw_id = art.get("data-adid")
            link_el = art.select_one("a.item-link")
            if not raw_id and link_el and link_el.get("href"):
                m = re.search(r"/immobile/(\d+)/", link_el["href"])
                if m:
                    raw_id = m.group(1)

            if not raw_id or not str(raw_id).isdigit():
                continue

            listing_id = IDEALISTA_ID_OFFSET + int(raw_id)

            # 2. Titolo e link
            title = link_el.get_text(strip=True) if link_el else art.get("title", "Annuncio Idealista")
            href = link_el.get("href", "") if link_el else ""
            full_url = f"https://www.idealista.it{href}" if href.startswith("/") else href

            # 3. Prezzo
            price_el = art.select_one(".item-price, .h2-simulated")
            price = None
            if price_el:
                price_text = re.sub(r"[^\d]", "", price_el.get_text(strip=True))
                if price_text.isdigit():
                    price = int(price_text)

            # 4. Calcolo geometrico del raggio (5.000 metri)
            # Cerca coordinate esatte nell'annuncio o usa il comune
            lat = art.get("data-latitude")
            lon = art.get("data-longitude")

            if lat and lon:
                dist = haversine_distance(CENTER_LAT, CENTER_LON, float(lat), float(lon))
            else:
                # Stima la posizione in base al comune menzionato nel titolo/indirizzo
                matched_dist = None
                text_lower = (title + " " + art.get_text(" ")).lower()
                for municipality, coords in MUNICIPALITY_COORDS.items():
                    if municipality in text_lower:
                        matched_dist = haversine_distance(CENTER_LAT, CENTER_LON, coords[0], coords[1])
                        break
                dist = matched_dist if matched_dist is not None else 0  # Se non specificato, non escluderlo a priori

            # Scarta matematicamente tutto ciò che supera i 5.000 metri
            if dist > MAX_DISTANCE_METERS:
                logger.debug(f"Scartato annuncio Idealista {raw_id}: distanza {dist:.0f}m > {MAX_DISTANCE_METERS}m")
                continue

            # 5. Dettagli (superficie, locali)
            details = [s.get_text(strip=True) for s in art.select(".item-detail, .item-detail-char span")]
            surface = next((d for d in details if "m²" in d or "mq" in d), None)
            rooms = next((d for d in details if "loc" in d or "stanze" in d), None)

            rows.append({
                "id": listing_id,
                "title": f"🏠 [Idealista] {title}",
                "price": price,
                "rent_eur": price,
                "surface": surface,
                "rooms": rooms,
                "url": full_url,
                "source": "idealista",
            })
        except Exception:
            continue

    logger.info(f"Idealista: estratti {len(rows)} annunci entro {MAX_DISTANCE_METERS}m")
    return rows
