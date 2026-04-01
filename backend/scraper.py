import asyncio
import logging
import random
import re
import httpx
from database import get_conn, put_conn

logger = logging.getLogger(__name__)

USER_AGENTS = [
    (
        "chrome-windows",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ),
    (
        "chrome-macos",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ),
    (
        "firefox-windows",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    ),
    (
        "firefox-macos",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    ),
    (
        "safari-macos",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    ),
    (
        "edge-windows",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    ),
]

DEFAULT_ACCEPT_LANGUAGE = "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7"


class RateLimitError(RuntimeError):
    pass


def build_session_headers():
    session_name, user_agent = random.choice(USER_AGENTS)
    search_headers = {
        "User-Agent": user_agent,
        "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
        "Accept": "application/json, text/plain, */*",
        "Referer": NEXT_BOOTSTRAP_URL,
    }
    page_headers = {
        "User-Agent": user_agent,
        "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Referer": NEXT_BOOTSTRAP_URL,
    }
    detail_headers = {
        "User-Agent": user_agent,
        "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
        "Accept": "application/json, text/plain, */*",
        "Referer": NEXT_BOOTSTRAP_URL,
    }
    return {
        "session_name": session_name,
        "search": search_headers,
        "page": page_headers,
        "detail": detail_headers,
    }


def raise_for_rate_limit(response, context):
    if response.status_code == 429:
        raise RateLimitError(f"HTTP 429 mentre {context}: {response.url}")

NEXT_BOOTSTRAP_URL = "https://www.immobiliare.it/affitto-case/napoli/"
NEXT_BUILD_ID = None
DETAIL_DESCRIPTION_MIN_LENGTH = 220

FALLBACK_IMAGE = "https://images.unsplash.com/photo-1514888286974-6c03e2ca1dba?ixlib=rb-4.1.0&auto=format&fit=crop&q=80&w=1143"

RICERCHE = [
    {
        "tipo": "casa",
        "url": (
            "https://www.immobiliare.it/api-next/search-list/listings/?"
            "raggio=10000"
            "&centro=40.87414%2C14.34105"
            "&idContratto=2"
            "&idCategoria=1"
            "&prezzoMassimo=1000"
            "&arredato=on"
            "&__lang=it"
            "&pag={}"
            "&paramsCount=8"
            "&path=%2Fsearch-list%2F"
        )
    },
    {
        "tipo": "stanza",
        "url": (
            "https://www.immobiliare.it/api-next/search-list/listings/?"
            "raggio=50000"
            "&centro=40.84721%2C14.28198"
            "&idContratto=2"
            "&idCategoria=4"
            "&prezzoMassimo=300"
            "&__lang=it"
            "&pag={}"
            "&paramsCount=8"
            "&path=%2Fsearch-list%2F"
        )
    }
]


def upgrade_small_photo_url(url):
    if not url:
        return ""

    upgraded = url
    for source in ("/xxs-c.", "/xs-c.", "/s-c.", "/t-c."):
        if source in upgraded:
            upgraded = upgraded.replace(source, "/m-c.")
            break

    return upgraded


def normalize_description(text):
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    return cleaned


def parse_int(value):
    if value is None:
        return None

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, (int, float)):
        return int(value)

    match = re.search(r"\d+", str(value))
    if not match:
        return None

    return int(match.group())


def has_listing_feature(props, *, feature_type=None, keywords=None):
    feature_list = props.get("featureList", [])
    if feature_type and any(item.get("type") == feature_type for item in feature_list):
        return True

    if keywords:
        keywords = tuple(keyword.lower() for keyword in keywords)
        ga4_features = [str(feature).lower() for feature in props.get("ga4features", [])]
        if any(keyword in feature for feature in ga4_features for keyword in keywords):
            return True

    return False


def extract_listing_attributes(props):
    parking_value = str(props.get("ga4Garage") or "").strip().lower()
    location = props.get("location", {})

    return {
        "stanze": parse_int(props.get("rooms")),
        "superficie": parse_int(props.get("surface")),
        "arredato": has_listing_feature(props, feature_type="furniture", keywords=("arredato",)),
        "ascensore": bool(props.get("elevator")) or has_listing_feature(
            props,
            feature_type="elevator",
            keywords=("ascensore",),
        ),
        "terrazzo": has_listing_feature(props, feature_type="terrace", keywords=("terrazzo",)),
        "posto_auto": (
            bool(parking_value and parking_value != "nessuno")
            or has_listing_feature(props, keywords=("garage", "posto auto", "parcheggio", "box"))
        ),
        "zona": location.get("city", ""),
        "macrozona": location.get("macrozone", ""),
        "microzona": location.get("microzone", ""),
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
    }


def extract_image_urls(props):
    image_urls = []
    seen_keys = set()

    def add(url, photo_id=None):
        key = photo_id or url
        if not url or key in seen_keys:
            return
        seen_keys.add(key)
        image_urls.append(url)

    main_photo_data = props.get("photo", {})
    main_photo = main_photo_data.get("urls", {})
    add(
        main_photo.get("large") or main_photo.get("medium") or main_photo.get("small"),
        main_photo_data.get("id"),
    )

    multimedia = props.get("multimedia", {})
    for photo in multimedia.get("photos", []):
        photo_urls = photo.get("urls", {})
        add(
            photo_urls.get("large")
            or photo_urls.get("medium")
            or upgrade_small_photo_url(photo_urls.get("small"))
            or photo_urls.get("small"),
            photo.get("id"),
        )

    if not image_urls:
        image_urls.append(FALLBACK_IMAGE)

    return image_urls


async def fetch_next_build_id(client, page_headers, force_refresh=False):
    global NEXT_BUILD_ID

    if NEXT_BUILD_ID and not force_refresh:
        return NEXT_BUILD_ID

    response = await client.get(NEXT_BOOTSTRAP_URL, headers=page_headers, timeout=20)
    raise_for_rate_limit(response, "caricavo il bootstrap HTML")
    response.raise_for_status()

    match = re.search(r'"buildId":"([^"]+)"', response.text)
    if not match:
        raise ValueError("Impossibile ricavare il buildId di Immobiliare.it")

    NEXT_BUILD_ID = match.group(1)
    return NEXT_BUILD_ID


async def fetch_detail_description(client, annuncio_id, page_headers, detail_headers):
    try:
        build_id = await fetch_next_build_id(client, page_headers=page_headers)
    except RateLimitError:
        raise
    except Exception as exc:
        logger.warning("Impossibile ricavare il buildId per l'annuncio %s: %s", annuncio_id, exc)
        return ""

    detail_url = f"https://www.immobiliare.it/_next/data/{build_id}/annunci/{annuncio_id}.json"
    response = await client.get(detail_url, headers=detail_headers, timeout=20, follow_redirects=True)
    raise_for_rate_limit(response, f"recuperavo la descrizione di dettaglio per l'annuncio {annuncio_id}")

    if response.status_code == 404:
        try:
            build_id = await fetch_next_build_id(client, page_headers=page_headers, force_refresh=True)
        except RateLimitError:
            raise
        except Exception as exc:
            logger.warning(
                "Refresh buildId non riuscito per l'annuncio %s dopo 404 sul dettaglio: %s",
                annuncio_id,
                exc,
            )
            return ""

        detail_url = f"https://www.immobiliare.it/_next/data/{build_id}/annunci/{annuncio_id}.json"
        response = await client.get(detail_url, headers=detail_headers, timeout=20, follow_redirects=True)
        raise_for_rate_limit(response, f"riprovo la descrizione di dettaglio per l'annuncio {annuncio_id}")

    if response.status_code != 200:
        return ""

    try:
        payload = response.json()
    except ValueError:
        return ""

    properties = payload.get("pageProps", {}).get("detailData", {}).get("realEstate", {}).get("properties", [])
    if not properties:
        return ""

    return normalize_description(properties[0].get("description"))


async def fetch_detail_description_for_listing(annuncio_id):
    session_headers = build_session_headers()
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        return await fetch_detail_description(
            client,
            annuncio_id,
            page_headers=session_headers["page"],
            detail_headers=session_headers["detail"],
        )


async def sleep_between_detail_fetches():
    await asyncio.sleep(random.uniform(1.5, 4.0))


def should_refresh_description(saved_description):
    return len(normalize_description(saved_description)) < DETAIL_DESCRIPTION_MIN_LENGTH


async def scrape():
    conn = get_conn()
    try:
        cur = conn.cursor()
        nuovi = 0
        detail_fetch_count = 0
        cur.execute("SELECT id, descrizione FROM annunci")
        existing_descriptions = {row[0]: normalize_description(row[1]) for row in cur.fetchall()}
        session_headers = build_session_headers()
        logger.info("Avvio scraping Immobiliare.it con sessione User-Agent %s", session_headers["session_name"])

        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                for ricerca in RICERCHE:
                    current_page = 0
                    max_pages = 1

                    while current_page < max_pages:
                        current_page += 1

                        response = await client.get(
                            ricerca["url"].format(current_page),
                            headers=session_headers["search"],
                            timeout=20,
                        )
                        raise_for_rate_limit(
                            response,
                            f"recuperavo la pagina {current_page} della ricerca {ricerca['tipo']}",
                        )
                        response.raise_for_status()
                        data = response.json()

                        max_pages = data.get("maxPages", 1)

                        for result in data.get("results", []):
                            id_annuncio = None
                            try:
                                props = result["realEstate"]["properties"][0]
                                id_annuncio = result["realEstate"]["id"]
                                titolo = result["seo"]["anchor"]
                                url = result["seo"]["url"]
                                prezzo = props["price"]["value"]
                                attributes = extract_listing_attributes(props)
                                image_urls = extract_image_urls(props)
                                url_immagine = image_urls[0]
                                descrizione_breve = normalize_description(props.get("description") or props.get("caption"))
                                descrizione = existing_descriptions.get(id_annuncio, "")
                                if should_refresh_description(descrizione):
                                    if detail_fetch_count > 0:
                                        await sleep_between_detail_fetches()
                                    descrizione = await fetch_detail_description(
                                        client,
                                        id_annuncio,
                                        page_headers=session_headers["page"],
                                        detail_headers=session_headers["detail"],
                                    ) or descrizione_breve
                                    detail_fetch_count += 1
                                elif not descrizione:
                                    descrizione = descrizione_breve

                                cur.execute("""
                                    INSERT INTO annunci (
                                        id,
                                        titolo,
                                        prezzo,
                                        stanze,
                                        superficie,
                                        arredato,
                                        ascensore,
                                        terrazzo,
                                        posto_auto,
                                        url,
                                        url_immagine,
                                        url_immagini,
                                        descrizione,
                                        zona,
                                        macrozona,
                                        microzona,
                                        latitude,
                                        longitude,
                                        tipo,
                                        created_at,
                                        updated_at
                                    )
                                    VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                                    )
                                    ON CONFLICT (id) DO UPDATE
                                    SET titolo = EXCLUDED.titolo,
                                        prezzo = EXCLUDED.prezzo,
                                        stanze = EXCLUDED.stanze,
                                        superficie = EXCLUDED.superficie,
                                        arredato = EXCLUDED.arredato,
                                        ascensore = EXCLUDED.ascensore,
                                        terrazzo = EXCLUDED.terrazzo,
                                        posto_auto = EXCLUDED.posto_auto,
                                        url = EXCLUDED.url,
                                        url_immagine = EXCLUDED.url_immagine,
                                        url_immagini = EXCLUDED.url_immagini,
                                        descrizione = EXCLUDED.descrizione,
                                        zona = EXCLUDED.zona,
                                        macrozona = EXCLUDED.macrozona,
                                        microzona = EXCLUDED.microzona,
                                        latitude = EXCLUDED.latitude,
                                        longitude = EXCLUDED.longitude,
                                        tipo = EXCLUDED.tipo,
                                        updated_at = NOW()
                                """, (
                                    id_annuncio,
                                    titolo,
                                    prezzo,
                                    attributes["stanze"],
                                    attributes["superficie"],
                                    attributes["arredato"],
                                    attributes["ascensore"],
                                    attributes["terrazzo"],
                                    attributes["posto_auto"],
                                    url,
                                    url_immagine,
                                    image_urls,
                                    descrizione,
                                    attributes["zona"],
                                    attributes["macrozona"],
                                    attributes["microzona"],
                                    attributes["latitude"],
                                    attributes["longitude"],
                                    ricerca["tipo"],
                                ))
                                existing_descriptions[id_annuncio] = descrizione
                                nuovi += 1
                            except RateLimitError:
                                raise
                            except Exception as exc:
                                logger.warning(
                                    "Salto annuncio %s della ricerca %s per errore di parsing: %s",
                                    id_annuncio or "sconosciuto",
                                    ricerca["tipo"],
                                    exc,
                                )
                                continue
        except RateLimitError as exc:
            logger.critical("Scraping interrotto per rate limit (HTTP 429): %s", exc)
            conn.commit()
            return nuovi

        conn.commit()
        return nuovi

    finally:
        put_conn(conn)
