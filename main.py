"""Render web service with a single browser scan every three minutes."""

import logging
import os
import signal
import threading
from pathlib import Path

from flask import Flask, jsonify
from waitress import serve

from service import Store, run_schedule

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def create_app(store, worker, interval):
    app = Flask(__name__)

    @app.get("/")
    @app.get("/health")
    def health():
        status = store.status()
        status.update(
            worker_alive=worker.is_alive(), interval_seconds=interval,
            revision=os.environ.get("RENDER_GIT_COMMIT", "unknown"),
            display=os.environ.get("DISPLAY"),
        )
        return jsonify(status), 200 if worker.is_alive() else 503

    return app


def main():
    directory = Path(os.environ.get("DATA_DIR", "data"))
    directory.mkdir(parents=True, exist_ok=True)
    interval = int(os.environ.get("SCRAPE_INTERVAL_SECONDS", "180"))
    if interval < 180:
        raise ValueError("SCRAPE_INTERVAL_SECONDS must be at least 180")
    executable = os.environ.get("CHROME_EXECUTABLE", "/usr/bin/chromium")
    if not Path(executable).is_file():
        raise FileNotFoundError(f"Browser executable missing: {executable}")
    store = Store(directory / "listings.sqlite3")
    stop = threading.Event()
    worker = threading.Thread(
        target=run_schedule, args=(store, executable, directory / "browser-profile", stop, interval),
        name="scraper", daemon=True,
    )

    def shutdown(*_):
        stop.set()
        worker.join(timeout=135)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    worker.start()
    try:
        serve(create_app(store, worker, interval), host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
    finally:
        stop.set()
        worker.join(timeout=135)


if __name__ == "__main__":
    main()
