import asyncio
import html
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
    api_id=os.getenv("api_id"),
    api_hash=os.getenv("api_hash"),
    bot_token=os.getenv("bot_token"),
)

CHAT_ID = int(os.getenv("chat_id"))

# =========================================================
# RICERCA IMMOBILIARE.IT
# =========================================================

URL_RICERCA_CASE = (
    "https://www.immobiliare.it/api-next/search-list/listings/"
)

SEARCH_PARAMS = {
    # 5 km dal nostro riferimento a Volla
    "raggio": 5000,
    "centro": "40.87414,14.34105",

    # Affitto
    "idContratto": 2,

    # Residenziale
    "idCategoria": 1,

    # Massimo 700 € di canone pubblicizzato
    "prezzoMassimo": 700,

    # Arredato
    "arredato": "on",

    # Più recenti
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
    return str(key).replace("_", "").replace("-", "").lower()


def find_values(data, wanted_keys):
    """
    Cerca ricorsivamente determinate chiavi all'interno del JSON.
    Restituisce [(chiave, valore), ...].
    """
    wanted = {
        normalize_key(key)
        for key in wanted_keys
    }

    found = []

    if isinstance(data, dict):
        for key, value in data.items():
            if normalize_key(key) in wanted:
                found.append((key, value))

            found.extend(
                find_values(value, wanted_keys)
            )

    elif isinstance(data, list):
        for item in data:
            found.extend(
                find_values(item, wanted_keys)
            )

    return found


def get_advertiser_status(result):
    """
    Restituisce:
    - private
    - professional
    - unknown
    """

    # Campo più affidabile quando presente
    private_values = find_values(
        result,
        {"isPrivate", "ownerDirect"}
    )

    for _, value in private_values:
        if value is True:
            return "private"

        if value is False:
            return "professional"

    # Altri possibili indicatori
    type_values = find_values(
        result,
        {
            "advertiserType",
            "agencyType",
            "clientType",
        }
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

    # Se esiste chiaramente un nome agenzia,
    # possiamo considerarlo professionale
    agency_values = find_values(
        result,
        {
            "agencyName",
            "agency",
            "officeName",
        }
    )

    for _, value in agency_values:
        if isinstance(value, str) and value.strip():
            return "professional"

    return "unknown"


def get_agency_name(result):
    values = find_values(
        result,
        {
            "agencyName",
            "officeName",
        }
    )

    for _, value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None


def parse_money(value):
    """
    Converte ad esempio:
    100
    "100"
    "€ 100/mese"
    in float.
    """

    if isinstance(value, (int, float)):
        return float(value)

    if not isinstance(value, str):
        return None

    cleaned = (
        value
        .replace(".", "")
        .replace(",", ".")
    )

    match = re.search(
        r"(\d+(?:\.\d+)?)",
        cleaned
    )

    if not match:
        return None

    try:
        return float(match.group(1))
    except ValueError:
        return None


def get_condominium_expenses(result):
    """
    Cerca solo campi esplicitamente riferiti
    alle spese condominiali.

    Non usiamo una generica chiave 'expenses'
    perché potrebbe indicare altro.
    """

    values = find_values(
        result,
        {
            "condominiumExpenses",
            "condominiumExpense",
            "monthlyCondominiumExpenses",
            "speseCondominiali",
        }
    )

    for _, value in values:
        parsed = parse_money(value)

        if parsed is not None:
            return parsed

    return None


def get_surface(result):
    values = find_values(
        result,
        {
            "surfaceValue",
            "surface",
            "surfaceM2",
        }
    )

    for _, value in values:
        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, str):
            match = re.search(r"(\d+)", value)

            if match:
                return int(match.group(1))

    return None


def format_euro(value):
    if value is None:
        return "N/D"

    if float(value).is_integer():
        return f"{int(value)} €"

    return f"{value:.2f} €".replace(".", ",")


# =========================================================
# SCRAPING
# =========================================================

async def scrape():
    conn = connection_pool.getconn()

    try:
        cur = conn.cursor()

        await app.send_message(
            chat_id=CHAT_ID,
            text="🔎 Inizio ricerca case...",
        )

        current_page = 0
        max_pages = 1

        async with httpx.AsyncClient(
                timeout=20,
                follow_redirects=True,
        ) as client:

            while current_page < max_pages:
                current_page += 1

                params = SEARCH_PARAMS.copy()
                params["pag"] = current_page

                response = await client.get(
                    URL_RICERCA_CASE,
                    params=params,
                    headers=HEADERS,
                )

                response.raise_for_status()

                data = response.json()

                max_pages = data.get(
                    "maxPages",
                    1,
                )

                for result in data.get("results", []):
                    real_estate = result.get(
                        "realEstate",
                        {}
                    )

                    properties = real_estate.get(
                        "properties",
                        []
                    )

                    if not properties:
                        continue

                    property_data = properties[0]

                    id_result = real_estate.get("id")

                    if id_result is None:
                        continue

                    # ---------------------------
                    # Già visto?
                    # ---------------------------

                    cur.execute(
                        SELECT_ANNUNCIO,
                        (id_result,),
                    )

                    if cur.fetchone() is not None:
                        continue

                    # ---------------------------
                    # Inserzionista
                    # ---------------------------

                    advertiser_status = (
                        get_advertiser_status(result)
                    )

                    # Se siamo sicuri che sia privato,
                    # non lo vogliamo.
                    if advertiser_status == "private":
                        continue

                    # ---------------------------
                    # Informazioni base
                    # ---------------------------

                    seo = result.get("seo", {})

                    url_result = seo.get(
                        "url",
                        f"https://www.immobiliare.it/annunci/{id_result}/",
                    )

                    title = seo.get(
                        "anchor",
                        "Annuncio Immobiliare.it",
                    )

                    price_data = property_data.get(
                        "price",
                        {}
                    )

                    price = price_data.get("value")

                    if price is None:
                        continue

                    # Sicurezza aggiuntiva
                    if price > 700:
                        continue

                    # ---------------------------
                    # Spese
                    # ---------------------------

                    condominium_expenses = (
                        get_condominium_expenses(result)
                    )

                    known_monthly_cost = price

                    if condominium_expenses is not None:
                        known_monthly_cost += (
                            condominium_expenses
                        )

                        # Se già canone + condominio
                        # supera il nostro limite,
                        # possiamo eliminarlo.
                        if known_monthly_cost > 700:
                            continue

                    # ---------------------------
                    # Altri dati
                    # ---------------------------

                    surface = get_surface(result)
                    agency_name = get_agency_name(result)

                    photo = property_data.get("photo")

                    if photo:
                        url_image = (
                            photo
                            .get("urls", {})
                            .get("medium")
                        )
                    else:
                        url_image = None

                    # ---------------------------
                    # Testo inserzionista
                    # ---------------------------

                    if advertiser_status == "professional":
                        advertiser_text = "🏢 Professionista / agenzia"
                    else:
                        advertiser_text = (
                            "⚠️ Inserzionista da verificare"
                        )

                    if agency_name:
                        advertiser_text += (
                            f" — {html.escape(agency_name)}"
                        )

                    # ---------------------------
                    # Testo spese
                    # ---------------------------

                    if condominium_expenses is not None:
                        expenses_text = (
                            f"🏢 <b>Condominio:</b> "
                            f"{format_euro(condominium_expenses)}/mese\n"
                            f"💰 <b>Totale noto:</b> "
                            f"{format_euro(known_monthly_cost)}/mese"
                        )
                    else:
                        expenses_text = (
                            "⚠️ <b>Spese condominiali:</b> "
                            "non determinate"
                        )

                    # ---------------------------
                    # Superficie
                    # ---------------------------

                    if surface is not None:
                        surface_text = (
                            f"📐 <b>Superficie:</b> "
                            f"{surface} m²\n"
                        )
                    else:
                        surface_text = ""

                    # ---------------------------
                    # Immagine
                    # ---------------------------

                    image_html = ""

                    if url_image:
                        image_html = (
                            f'<a href="{url_image}">🏠</a> '
                        )

                    # ---------------------------
                    # Telegram
                    # ---------------------------

                    message = (
                        f"{image_html}"
                        f"<b>{html.escape(title)}</b>\n\n"
                        f"💶 <b>Canone:</b> "
                        f"{format_euro(price)}/mese\n"
                        f"{surface_text}"
                        f"{expenses_text}\n\n"
                        f"{advertiser_text}\n\n"
                        f'🔗 <b><a href="{url_result}">'
                        f"Apri annuncio"
                        f"</a></b>"
                    )

                    await app.send_message(
                        chat_id=CHAT_ID,
                        text=message,
                    )

                    # Lo salviamo solo dopo
                    # aver mandato correttamente
                    # la notifica.
                    cur.execute(
                        INSERT_ANNUNCIO,
                        (id_result,),
                    )

                    await asyncio.sleep(0.5)

        conn.commit()

    except Exception as e:
        conn.rollback()

        await app.send_message(
            chat_id=CHAT_ID,
            text=(
                "❌ Errore durante lo scrape:\n"
                f"<code>{html.escape(str(e))}</code>"
            ),
        )

    finally:
        connection_pool.putconn(conn)


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

keep_alive()

app.run()
