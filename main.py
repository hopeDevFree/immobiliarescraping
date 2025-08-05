import asyncio
import os
from datetime import datetime, timedelta

import httpx
import psycopg2
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from psycopg2 import pool
from pyrogram import Client

from keep_alive import keep_alive

load_dotenv()

connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10,
    dbname=os.getenv("db_name"),
    user=os.getenv("db_user"),
    password=os.getenv("db_password"),
    host=os.getenv("db_host"),
    port=os.getenv("db_port")
)

insert_into_annunci = """
        INSERT INTO annunci(id)
        VALUES (%s)
    """

select_from_annunci = """ SELECT * 
FROM annunci 
WHERE id = %s
"""

app = Client(name=os.getenv("client_name"), api_id=os.getenv("api_id"), api_hash=os.getenv("api_hash"),
             bot_token=os.getenv("bot_token"))

url_ricerca_case = ("https://www.immobiliare.it/api-next/search-list/listings/?"
                    "raggio=10000"
                    "&centro=40.87414%2C14.34105"
                    "&idContratto=2"
                    "&idCategoria=1"
                    "&prezzoMassimo=1000"
                    "&arredato=on"
                    "&__lang=it"
                    "&pag={}"
                    "&paramsCount=8"
                    "&path=%2Fsearch-list%2F")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}


async def scrape():
    conn = connection_pool.getconn()
    try:
        cur = conn.cursor()

        await app.send_message(chat_id=5239432590, text=f"""--- Inizio scrape ---""")

        current_page = 0
        max_pages = 1
        while current_page < max_pages:

            current_page = current_page + 1

            async with httpx.AsyncClient() as client:
                response = await client.get(url_ricerca_case.format(current_page), headers=headers)
                json = response.json()

            max_pages = json['maxPages']
            for result in json['results']:
                url_result = result['seo']['url']

                if 'photo' in result['realEstate']['properties'][0]:
                    url_image = result['realEstate']['properties'][0]['photo']['urls']['medium']
                else:
                    url_image = "https://unsplash.com/it/foto/gatto-in-bianco-e-nero-sdraiato-su-una-sedia-di-bambu-marrone-allinterno-della-stanza-gKXKBY-C-Dk"
                price = result['realEstate']['properties'][0]['price']['value']
                title = result['seo']['anchor']
                id_result = result['realEstate']['id']

                cur.execute(select_from_annunci, (id_result,))
                exists = cur.fetchone()
                if exists is None:
                    cur.execute(insert_into_annunci, (id_result,))
                    await app.send_message(chat_id=5239432590,
                                           text=f"""<a href={url_image}>🏠</a> <b>{title}</b>
💲 <b>Prezzo</b>: € {price}/mese
    
🔗 <b><i><a href={url_result}>Link</a></i></b>
""")

            conn.commit()
    except Exception as e:
        await app.send_message(chat_id=5239432590,
                               text=f"""Errore: {e}""")

    finally:
        connection_pool.putconn(conn)


loop = asyncio.get_event_loop()
scheduler = AsyncIOScheduler(timezone="Europe/Rome", event_loop=loop)
scheduler.add_job(scrape, "interval", minutes=10, next_run_time=datetime.now() + timedelta(seconds=30))
scheduler.start()
keep_alive()
app.run()
