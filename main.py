from contextlib import closing

import httpx
from telegraph import Telegraph

from pyrogram import Client, idle
import tgcrypto
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

from dotenv import load_dotenv
from keep_alive import keep_alive
import psycopg2

load_dotenv()

app = Client("immobiliarescrape",
             api_id=os.environ['api_id'],
             api_hash=os.environ['api_hash'],
             bot_token=os.environ['bot_token'])

scheduler = AsyncIOScheduler(timezone="Europe/Rome")


async def scrape():
    with closing(psycopg2.connect(
            database=os.environ["db_name"],
            host=os.environ["db_host"],
            user=os.environ["db_user"],
            password=os.environ["db_password"],
            port=os.environ["db_port"]
    )) as conn:
        with conn.cursor() as cur:

            immo_url = ("https://www.immobiliare.it/api-next/search-list/real-estates/?"
                        "raggio=5000"
                        "&centro=40.87414%2C14.34105"
                        "&idContratto=2"
                        "&idCategoria=1"
                        "&prezzoMassimo=800"
                        "&criterio=prezzo"
                        "&ordine=asc"
                        "&arredato=on"
                        "&__lang=it"
                        "&pag=1"
                        "&paramsCount=11"
                        "&path=%2Fsearch-list%2F")

            async with httpx.AsyncClient() as client:
                response = await client.get(immo_url)
                json_test = response.json()

                current_page = json_test['currentPage']
                max_pages = json_test['maxPages']

                while current_page <= max_pages:

                    for result in json_test['results']:
                        cur.execute("SELECT * FROM annunci WHERE url = %s", (result['seo']['url'],))
                        if cur.fetchone() is None and 'photo' in result['realEstate']['properties'][0]:
                            await app.send_message(chat_id='@immobiliarescrape',
                                                   text=f"""🏠 <b>Nuovo annuncio!</b>
<a href='{result['realEstate']['properties'][0]['photo']['urls']['large']}'> </a>
🔗 <a href='{result["seo"]["url"]}'>{result["seo"]["title"]}</a>

💶 <b>Prezzo</b>: {result['realEstate']['price']['formattedValue']}
""",
                                                   disable_web_page_preview=False)

                            cur.execute("INSERT INTO annunci(url) values(%s)", (result['seo']['url'],))
                        conn.commit()

                    if current_page <= max_pages:
                        current_page = current_page + 1

                        immo_url = ("https://www.immobiliare.it/api-next/search-list/real-estates/?"
                                    "raggio=5000"
                                    "&centro=40.87414%2C14.34105"
                                    "&idContratto=2"
                                    "&idCategoria=1"
                                    "&prezzoMassimo=800"
                                    "&criterio=prezzo"
                                    "&ordine=asc"
                                    "&arredato=on"
                                    "&__lang=it"
                                    f"&pag={current_page}"
                                    "&paramsCount=11"
                                    "&path=%2Fsearch-list%2F")

                        response = await client.get(immo_url)
                        json_test = response.json()

                await app.send_message(chat_id='5453376840',
                                       text="Finito scraping")


app.start()
scheduler.add_job(scrape, "interval", minutes=30, next_run_time=datetime.now() + timedelta(seconds=10))
scheduler.start()
keep_alive()
idle()
