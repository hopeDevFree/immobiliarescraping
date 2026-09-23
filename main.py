import asyncio
import html
import logging
import os
import re
from datetime import datetime, timedelta

import httpx
import psycopg2
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from psycopg2 import pool
from pyrogram import Client

from keep_alive import keep_alive

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# =========================================================
# DATABASE
# =========================================================

connection_pool = psycopg2.pool.SimpleConnectionPool(
    1,
    10,
    dbname=os.getenv("db_name"),
    user=os.getenv("db_user"),
    password=os.getenv("db_password"),
    host=os.getenv("db_host"),
    port=os.getenv("db_port"),
)

INSERT_ANNUNCIO = """
    INSERT INTO annunci(id)
    VALUES (%s)
"""

SELECT_ANNUNCIO = """
    SELECT id
    FROM annunci
    WHERE id = %s
"""

# =========================================================
# TELEGRAM
# =========================================================

app = Client(
    name=os.getenv("client_name"),
    api_id=int(os.getenv("api_id")),
    api_hash=os.getenv("api_hash"),
    bot_token=os.getenv("bot_token"),
)

CHAT_ID = int(os.getenv("chat_id"))


async def send_telegram_message(text):
    """
    Prova a inviare un messaggio Telegram.

    Se Telegram non funziona, l'errore viene scritto nei log
    senza interrompere il resto del bot.
    """

    try:
        await app.send_message(
            chat_id=CHAT_ID,
            text=text,
        )

        return True

    except Exception:
        logger.exception(
            "Impossibile inviare il messaggio Telegram"
        )

        return False


# =========================================================
# IMMOBILIARE.IT
# =========================================================

URL_RICERCA_CASE = (
    "https://www.immobiliare.it/api-next/search-list/listings/"
)

SEARCH_PARAMS = {
    # Raggio: 5 km
    "raggio": 5000,
    "centro": "40.87414,14.34105",

    # Affitto
    "idContratto": 2,

    # Immobili residenziali
    "idCategoria": 1,

    # Massimo 700 € di canone
    "prezzoMassimo": 700,

    # Arredato
    "arredato": "on",

    # Annunci più recenti
    "criterio": "data",
    "ordine": "desc",

    "__lang": "it",

    "path": "/affitto-case/",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/153.0.0.0 Safari/537.36"
    )
}


# =========================================================
# FUNZIONI DI SUPPORTO
# =========================================================

def normalize_key(key):
    return (
        str(key)
        .replace("_", "")
        .replace("-", "")
        .lower()
    )


def find_values(data, wanted_keys):
    """
    Cerca ricorsivamente determinate chiavi
    all'interno di un dict/list JSON.

    Restituisce una lista:
    [
        (chiave, valore),
        ...
    ]
    """

    wanted = {
        normalize_key(key)
        for key in wanted_keys
    }

    found = []

    if isinstance(data, dict):

        for key, value in data.items():

            if normalize_key(key) in wanted:
                found.append(
                    (key, value)
                )

            found.extend(
                find_values(
                    value,
                    wanted_keys,
                )
            )

    elif isinstance(data, list):

        for item in data:

            found.extend(
                find_values(
                    item,
                    wanted_keys,
                )
            )

    return found


def get_advertiser_status(result):
    """
    Restituisce:

    private
    professional
    unknown
    """

    private_values = find_values(
        result,
        {
            "isPrivate",
            "ownerDirect",
        },
    )

    for _, value in private_values:

        if value is True:
            return "private"

        if value is False:
            return "professional"

    type_values = find_values(
        result,
        {
            "advertiserType",
            "agencyType",
            "clientType",
        },
    )

    for _, value in type_values:

        if not isinstance(value, str):
            continue

        value = value.strip().lower()

        if value in {
            "private",
            "privato",
            "private_owner",
            "owner",
        }:
            return "private"

        if value in {
            "agency",
            "agenzia",
            "professional",
            "professionista",
            "company",
            "impresa",
            "constructor",
            "costruttore",
            "builder",
        }:
            return "professional"

    agency_values = find_values(
        result,
        {
            "agencyName",
            "agency",
            "officeName",
        },
    )

    for _, value in agency_values:

        if (
            isinstance(value, str)
            and value.strip()
        ):
            return "professional"

    return "unknown"


def get_agency_name(result):
    values = find_values(
        result,
        {
            "agencyName",
            "officeName",
        },
    )

    for _, value in values:

        if (
            isinstance(value, str)
            and value.strip()
        ):
            return value.strip()

    return None


def parse_money(value):
    """
    Converte valori come:

    100
    "100"
    "€ 100/mese"

    in float.
    """

    if isinstance(
        value,
        (int, float),
    ):
        return float(value)

    if not isinstance(
        value,
        str,
    ):
        return None

    cleaned = (
        value
        .replace(".", "")
        .replace(",", ".")
    )

    match = re.search(
        r"(\d+(?:\.\d+)?)",
        cleaned,
    )

    if not match:
        return None

    try:
        return float(
            match.group(1)
        )

    except ValueError:
        return None


def get_condominium_expenses(result):
    """
    Cerca eventuali spese condominiali
    dichiarate nel JSON.
    """

    values = find_values(
        result,
        {
            "condominiumExpenses",
            "condominiumExpense",
            "monthlyCondominiumExpenses",
            "speseCondominiali",
        },
    )

    for _, value in values:

        parsed = parse_money(value)

        if parsed is not None:
            return parsed

    return None


def get_surface(result):
    """
    Cerca la superficie dell'immobile.
    """

    values = find_values(
        result,
        {
            "surfaceValue",
            "surface",
            "surfaceM2",
        },
    )

    for _, value in values:

        if isinstance(
            value,
            (int, float),
        ):
            return int(value)

        if isinstance(
            value,
            str,
        ):

            match = re.search(
                r"(\d+)",
                value,
            )

            if match:
                return int(
                    match.group(1)
                )

    return None


def format_euro(value):
    if value is None:
        return "N/D"

    value = float(value)

    if value.is_integer():
        return f"{int(value)} €"

    return (
        f"{value:.2f} €"
        .replace(".", ",")
    )


# =========================================================
# SCRAPING
# =========================================================

async def scrape():

    logger.info(
        "================================================="
    )

    logger.info(
        "Inizio ricerca case"
    )

    conn = None

    try:

        # -------------------------------------------------
        # DATABASE CONNECTION
        # -------------------------------------------------

        conn = connection_pool.getconn()

        cur = conn.cursor()

        logger.info(
            "Connessione database ottenuta"
        )

        # Telegram è solo informativo.
        await send_telegram_message(
            "🔎 Inizio ricerca case..."
        )

        current_page = 0
        max_pages = 1

        total_results = 0
        new_results = 0
        private_results = 0
        over_budget_results = 0
        already_seen_results = 0

        async with httpx.AsyncClient(
            timeout=20,
            follow_redirects=True,
        ) as client:

            while current_page < max_pages:

                current_page += 1

                logger.info(
                    "Richiesta pagina %s",
                    current_page,
                )

                params = SEARCH_PARAMS.copy()

                params["pag"] = current_page

                response = await client.get(
                    URL_RICERCA_CASE,
                    params=params,
                    headers=HEADERS,
                )

                logger.info(
                    "Immobiliare.it HTTP %s",
                    response.status_code,
                )

                response.raise_for_status()

                data = response.json()

                max_pages = data.get(
                    "maxPages",
                    1,
                )

                results = data.get(
                    "results",
                    [],
                )

                logger.info(
                    "Pagina %s/%s - %s annunci",
                    current_page,
                    max_pages,
                    len(results),
                )

                for result in results:

                    total_results += 1

                    real_estate = result.get(
                        "realEstate",
                        {},
                    )

                    properties = real_estate.get(
                        "properties",
                        [],
                    )

                    if not properties:

                        logger.warning(
                            "Annuncio senza properties"
                        )

                        continue

                    property_data = properties[0]

                    id_result = real_estate.get(
                        "id"
                    )

                    if id_result is None:

                        logger.warning(
                            "Annuncio senza ID"
                        )

                        continue

                    # -------------------------------------
                    # GIÀ VISTO?
                    # -------------------------------------

                    cur.execute(
                        SELECT_ANNUNCIO,
                        (id_result,),
                    )

                    if cur.fetchone() is not None:

                        already_seen_results += 1

                        logger.debug(
                            "Annuncio %s già visto",
                            id_result,
                        )

                        continue

                    # -------------------------------------
                    # INSERZIONISTA
                    # -------------------------------------

                    advertiser_status = (
                        get_advertiser_status(
                            result
                        )
                    )

                    if advertiser_status == "private":

                        private_results += 1

                        logger.info(
                            "Annuncio %s scartato: privato",
                            id_result,
                        )

                        continue

                    # -------------------------------------
                    # URL + TITOLO
                    # -------------------------------------

                    seo = result.get(
                        "seo",
                        {},
                    )

                    url_result = seo.get(
                        "url",
                        (
                            "https://www.immobiliare.it/"
                            f"annunci/{id_result}/"
                        ),
                    )

                    title = seo.get(
                        "anchor",
                        "Annuncio Immobiliare.it",
                    )

                    # -------------------------------------
                    # PREZZO
                    # -------------------------------------

                    price_data = property_data.get(
                        "price",
                        {},
                    )

                    price = price_data.get(
                        "value"
                    )

                    if price is None:

                        logger.warning(
                            "Annuncio %s senza prezzo",
                            id_result,
                        )

                        continue

                    try:
                        price = float(price)

                    except (TypeError, ValueError):

                        logger.warning(
                            "Prezzo non valido per annuncio %s: %s",
                            id_result,
                            price,
                        )

                        continue

                    if price > 700:

                        over_budget_results += 1

                        logger.info(
                            "Annuncio %s scartato: "
                            "prezzo %s €",
                            id_result,
                            price,
                        )

                        continue

                    # -------------------------------------
                    # SPESE CONDOMINIALI
                    # -------------------------------------

                    condominium_expenses = (
                        get_condominium_expenses(
                            result
                        )
                    )

                    known_monthly_cost = price

                    if (
                        condominium_expenses
                        is not None
                    ):

                        known_monthly_cost += (
                            condominium_expenses
                        )

                        if known_monthly_cost > 700:

                            over_budget_results += 1

                            logger.info(
                                "Annuncio %s scartato: "
                                "totale noto %s €",
                                id_result,
                                known_monthly_cost,
                            )

                            continue

                    # -------------------------------------
                    # DATI EXTRA
                    # -------------------------------------

                    surface = get_surface(
                        result
                    )

                    agency_name = (
                        get_agency_name(
                            result
                        )
                    )

                    photo = property_data.get(
                        "photo"
                    )

                    url_image = None

                    if photo:

                        url_image = (
                            photo
                            .get("urls", {})
                            .get("medium")
                        )

                    # -------------------------------------
                    # INSERZIONISTA TELEGRAM
                    # -------------------------------------

                    if (
                        advertiser_status
                        == "professional"
                    ):

                        advertiser_text = (
                            "🏢 Professionista / agenzia"
                        )

                    else:

                        advertiser_text = (
                            "⚠️ Inserzionista da verificare"
                        )

                    if agency_name:

                        advertiser_text += (
                            " — "
                            + html.escape(
                                agency_name
                            )
                        )

                    # -------------------------------------
                    # SPESE TELEGRAM
                    # -------------------------------------

                    if (
                        condominium_expenses
                        is not None
                    ):

                        expenses_text = (
                            "🏢 <b>Condominio:</b> "
                            f"{format_euro(condominium_expenses)}"
                            "/mese\n"
                            "💰 <b>Totale noto:</b> "
                            f"{format_euro(known_monthly_cost)}"
                            "/mese"
                        )

                    else:

                        expenses_text = (
                            "⚠️ <b>Spese condominiali:</b> "
                            "non determinate"
                        )

                    # -------------------------------------
                    # SUPERFICIE
                    # -------------------------------------

                    if surface is not None:

                        surface_text = (
                            "📐 <b>Superficie:</b> "
                            f"{surface} m²\n"
                        )

                    else:

                        surface_text = ""

                    # -------------------------------------
                    # IMMAGINE
                    # -------------------------------------

                    image_html = ""

                    if url_image:

                        image_html = (
                            f'<a href="{url_image}">'
                            "🏠"
                            "</a> "
                        )

                    # -------------------------------------
                    # MESSAGGIO
                    # -------------------------------------

                    message = (
                        f"{image_html}"
                        f"<b>{html.escape(title)}</b>\n\n"
                        "💶 <b>Canone:</b> "
                        f"{format_euro(price)}/mese\n"
                        f"{surface_text}"
                        f"{expenses_text}\n\n"
                        f"{advertiser_text}\n\n"
                        f'<b><a href="{url_result}">'
                        "🔗 Apri annuncio"
                        "</a></b>"
                    )

                    # -------------------------------------
                    # LOG SERVER
                    # -------------------------------------

                    logger.info(
                        "NUOVO ANNUNCIO | "
                        "id=%s | "
                        "prezzo=%s | "
                        "superficie=%s | "
                        "inserzionista=%s | "
                        "agenzia=%s | "
                        "url=%s",
                        id_result,
                        price,
                        surface,
                        advertiser_status,
                        agency_name,
                        url_result,
                    )

                    # -------------------------------------
                    # TELEGRAM
                    # -------------------------------------

                    telegram_sent = (
                        await send_telegram_message(
                            message
                        )
                    )

                    if telegram_sent:

                        logger.info(
                            "Notifica Telegram inviata "
                            "per annuncio %s",
                            id_result,
                        )

                    else:

                        logger.warning(
                            "Notifica Telegram NON inviata "
                            "per annuncio %s",
                            id_result,
                        )

                    # -------------------------------------
                    # DATABASE
                    # -------------------------------------

                    cur.execute(
                        INSERT_ANNUNCIO,
                        (id_result,),
                    )

                    conn.commit()

                    new_results += 1

                    logger.info(
                        "Annuncio %s salvato nel database",
                        id_result,
                    )

                    await asyncio.sleep(
                        0.5
                    )

        # -------------------------------------------------
        # RISULTATO SCRAPE
        # -------------------------------------------------

        logger.info(
            "Scrape completato | "
            "totali=%s | "
            "nuovi=%s | "
            "già_visti=%s | "
            "privati=%s | "
            "fuori_budget=%s",
            total_results,
            new_results,
            already_seen_results,
            private_results,
            over_budget_results,
        )

        await send_telegram_message(
            "✅ Ricerca completata\n\n"
            f"🏠 Nuovi annunci: {new_results}"
        )

    # =====================================================
    # ERRORE GENERALE
    # =====================================================

    except Exception as e:

        if conn is not None:

            try:
                conn.rollback()

            except Exception:
                logger.exception(
                    "Errore durante rollback database"
                )

        logger.exception(
            "Errore durante lo scrape"
        )

        await send_telegram_message(
            "❌ Errore durante lo scrape:\n"
            f"<code>{html.escape(str(e))}</code>"
        )

    # =====================================================
    # CLEANUP
    # =====================================================

    finally:

        if conn is not None:

            try:

                connection_pool.putconn(
                    conn
                )

                logger.info(
                    "Connessione database restituita al pool"
                )

            except Exception:

                logger.exception(
                    "Errore restituendo la connessione al pool"
                )

        logger.info(
            "Fine ciclo scrape"
        )

        logger.info(
            "================================================="
        )


# =========================================================
# SCHEDULER
# =========================================================

loop = asyncio.get_event_loop()

scheduler = AsyncIOScheduler(
    timezone="Europe/Rome",
    event_loop=loop,
)

scheduler.add_job(
    scrape,
    "interval",
    minutes=10,
    next_run_time=(
        datetime.now()
        + timedelta(seconds=30)
    ),
)

scheduler.start()

logger.info(
    "Scheduler avviato: scrape ogni 10 minuti"
)


# =========================================================
# KEEP ALIVE
# =========================================================

keep_alive()


# =========================================================
# START TELEGRAM
# =========================================================

logger.info(
    "Avvio client Telegram"
)

app.run()