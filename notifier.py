"""Invio Telegram dei nuovi annunci, aggregato e senza spam al primo avvio."""

import html
import logging
import os

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
BASE_URL = "https://www.immobiliare.it"
MAX_LISTINGS_PER_MESSAGE = 10
AGGREGATE_THRESHOLD = 3


def _send(html_text):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.warning("Telegram non configurato (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return False
    response = httpx.post(
        TELEGRAM_API.format(token=token),
        json={
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=15,
    )
    if response.status_code != 200:
        logger.warning("Telegram HTTP %s: %s", response.status_code, response.text[:300])
        return False
    return True


def _esc(value):
    return html.escape(str(value)) if value is not None else "—"


def _price(row):
    value = row.get("rent_eur") or row.get("price") or row.get("prezzo")
    if isinstance(value, (int, float)):
        return f"€{value:,.0f}".replace(",", ".")
    return _esc(value)


def _absolute_url(row):
    url = row.get("url") or row.get("link")
    if not url:
        return None
    url = str(url)
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return BASE_URL + url
    return f"{BASE_URL}/{url.lstrip('/')}"


def _zone_label(row):
    # Nel payload lista non c'è "zone". Usa la distanza se disponibile,
    # altrimenti mostra la prima feature utile. Adatta le chiavi a
    # search_config.distance_from_volla se i nomi sono diversi.
    for key in ("distance_from_volla_km", "distance_km", "distance"):
        value = row.get(key)
        if isinstance(value, (int, float)):
            return f"{value:.1f} km dal centro"
    features = row.get("features")
    if isinstance(features, list) and features:
        return ", ".join(str(f) for f in features[:2])
    return None


def _listing_block(row, index=None):
    title = row.get("title") or "Annuncio"
    url = _absolute_url(row)
    zone = _zone_label(row)
    surface = row.get("surface")
    rooms = row.get("rooms")

    prefix = f"{index}. " if index else ""
    lines = [f"🏠 <b>{prefix}{_esc(title)}</b>"]
    if zone:
        lines.append(f"📍 {_esc(zone)}")
    lines.append(f"💰 <b>{_price(row)}</b>/mese")
    details = []
    if surface:
        details.append(f"{_esc(surface)} m²")
    if rooms:
        details.append(f"{_esc(rooms)} locali")
    if details:
        lines.append("📐 " + " · ".join(details))
    if url:
        lines.append(f'<a href="{html.escape(url, quote=True)}">🔗 Vedi su Immobiliare.it</a>')
    return "\n".join(lines)


def notify_new_listings(new_rows, first_population=False):
    if not new_rows:
        return
    count = len(new_rows)

    if first_population:
        logger.info("NOTIFY_SKIPPED first_population total=%s", count)
        header = (f"✅ Prima scansione completata: <b>{count}</b> annunci salvati.\n"
                  f"Ecco un esempio formattato:\n\n")
        _send(header + _listing_block(new_rows[0]))
        return

    if count <= AGGREGATE_THRESHOLD:
        for row in new_rows:
            _send(_listing_block(row))
        logger.info("NOTIFY_SENT single count=%s", count)
        return

    for start in range(0, count, MAX_LISTINGS_PER_MESSAGE):
        chunk = new_rows[start:start + MAX_LISTINGS_PER_MESSAGE]
        header = f"🏠 <b>{count} nuovi annunci trovati</b>\n\n"
        body = "\n\n".join(
            _listing_block(row, index=start + i + 1) for i, row in enumerate(chunk)
        )
        _send(header + body)
    logger.info("NOTIFY_SENT aggregate count=%s", count)