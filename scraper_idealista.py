import json
import logging
import math
import os
import re
import urllib.error
import urllib.request
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Ricerca su tutta la provincia ordinata per i più recenti
IDEALISTA_URL = os.environ.get(
    "IDEALISTA_URL",
    "https://www.idealista.it/affitto-case/napoli-provincia/?ordine=pubblicazione-desc"
)

# Coordinate punto centrale (Sant'Anastasia)
CENTER_LAT = 40.87414
CENTER_LON = 14.34105
MAX_DISTANCE_METERS = 5000  # 5 km esatti

IDEALISTA_ID_OFFSET = 3_000_000_000

# Coordinate dei comuni della tua zona (per gli annunci senza civico esatto)
MUNICIPALITY_COORDS = {
    "sant'anastasia": (40.8741, 14.3411),
    "madonna dell'arco": (40.8705, 14.3312),
    "pollena trocchia": (40.8578, 14.3828),
    "somma vesuviana": (40.8722, 14.4372),
    "pomigliano d'arco": (40.9133, 14.3917),
    "pomigliano": (40.9133, 14.3917),
    "cercola": (40.8594, 14.3547),
    "massa di somma": (40.8492, 14.3725),
    "san sebastiano al vesuvio": (40.8419, 14.3644),
    "volla": (40.8833, 14.3333),
}


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcola la distanza geometrica esatta in metri."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def fetch_idealista_listings():
    jina_url = f"https://r.jina.ai/{IDEALISTA_URL}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
    except Exception as e:
        logger.warning(f"Idealista fetch non riuscito: {e}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    page_title = soup.title.get_text(strip=True) if soup.title else "Nessun titolo"
    logger.info("Idealista scaricato (%d bytes). Pagina: '%s'", len(html), page_title)

    if "captcha-delivery.com" in html or "datadome" in html.lower():
        logger.warning("Idealista: risposta con DataDome Captcha")
        return []

    articles = soup.select("article.item") or soup.select("div.item-info-container") or soup.select("[data-adid]")
    logger.info("Idealista: analizzo %d annunci recenti in provincia di Napoli...", len(articles))

    rows = []
    seen_ids = set()

    for art in articles:
        try:
            # 1. Ricava ID
            raw_id = art.get("data-adid")
            link_el = art.select_one("a.item-link, a[href*='/immobile/']")
            href = link_el.get("href", "") if link_el else ""

            if not raw_id and href:
                m = re.search(r"/immobile/(\d+)/", href)
                if m:
                    raw_id = m.group(1)

            if not raw_id or not str(raw_id).isdigit():
                continue

            listing_id = IDEALISTA_ID_OFFSET + int(raw_id)
            if listing_id in seen_ids:
                continue

            # 2. Titolo e link
            title = link_el.get_text(strip=True) if link_el else "Annuncio Idealista"
            full_url = f"https://www.idealista.it{href}" if href.startswith("/") else href

            # 3. Prezzo
            price_el = art.select_one(".item-price, .price, .h2-simulated")
            price = None
            if price_el:
                price_text = re.sub(r"[^\d]", "", price_el.get_text(strip=True))
                if price_text.isdigit():
                    price = int(price_text)

            # 4. Filtro geometrico (5.000 metri da Sant'Anastasia)
            lat = art.get("data-latitude")
            lon = art.get("data-longitude")
            text_lower = (title + " " + art.get_text(" ")).lower()

            if lat and lon:
                dist = haversine_distance(CENTER_LAT, CENTER_LON, float(lat), float(lon))
            else:
                # Se non c'è il GPS, controlliamo se cita uno dei nostri comuni limitrofi
                matched_dist = None
                for municipality, coords in MUNICIPALITY_COORDS.items():
                    if municipality in text_lower:
                        matched_dist = haversine_distance(CENTER_LAT, CENTER_LON, coords[0], coords[1])
                        break

                # Se l'annuncio è di un'altra zona della provincia (es. Pozzuoli, Sorrento, Giugliano, ecc.), SCARTALO
                if matched_dist is None:
                    continue
                dist = matched_dist

            # Scarta matematicamente tutto ciò che supera i 5.000 metri
            if dist > MAX_DISTANCE_METERS:
                continue

            # 5. Dettagli (superficie, vani)
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
            seen_ids.add(listing_id)
        except Exception:
            continue

    logger.info("Idealista: trovati %d annunci entro 5 km da Sant'Anastasia", len(rows))
    return rows