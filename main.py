import json
import logging
import os
import threading

from flask import Flask
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

web = Flask(__name__)


@web.route("/")
def home():
    return "Test Playwright attivo", 200


def test_playwright():
    logger.info("TEST PLAYWRIGHT | avvio")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)

            try:
                page = browser.new_page()
                response = page.goto(
                    "https://www.immobiliare.it/affitto-case/caserta/",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )

                status = response.status if response else None
                logger.info("TEST PLAYWRIGHT | HTTP %s", status)

                if status != 200:
                    logger.error("TEST PLAYWRIGHT | pagina non accessibile")
                    return

                node = page.locator("#__NEXT_DATA__")
                node.wait_for(state="attached", timeout=10000)

                data = json.loads(node.text_content())
                queries = data["props"]["pageProps"]["dehydratedState"]["queries"]

                counts = [
                    len(q["state"]["data"]["results"])
                    for q in queries
                    if isinstance(q.get("state", {}).get("data"), dict)
                    and isinstance(q["state"]["data"].get("results"), list)
                ]

                logger.info(
                    "TEST PLAYWRIGHT | annunci caricati: %s",
                    sum(counts),
                )
                logger.info("TEST PLAYWRIGHT | titolo: %s", page.title())

            finally:
                browser.close()

    except Exception:
        logger.exception("TEST PLAYWRIGHT | errore")


if __name__ == "__main__":
    test = threading.Timer(5, test_playwright)
    test.daemon = True
    test.start()

    web.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "10000")),
        use_reloader=False,
    )