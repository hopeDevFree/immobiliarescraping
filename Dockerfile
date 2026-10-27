FROM python:3.11-slim-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CHROME_EXECUTABLE=/usr/bin/chromium \
    DATA_DIR=/app/data \
    SCRAPE_INTERVAL_SECONDS=180

RUN apt-get update \
    && apt-get install -y --no-install-recommends chromium xvfb xauth tini fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-server.txt .
RUN pip install --no-cache-dir -r requirements-server.txt

RUN useradd --create-home --uid 10001 scraper \
    && mkdir -p /app/data && chown scraper:scraper /app/data
COPY --chown=scraper:scraper main.py service.py scraper.py browser_runtime.py listing_parser.py search_config.py access_http.py access_browser.py access_reader.py access_strategy.py notifier.py scraper_subito.py ./
USER scraper

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["xvfb-run", "--auto-servernum", "--server-args=-screen 0 1366x900x24", "python", "main.py"]