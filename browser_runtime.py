"""Own and reap Chromium across repeated scans in a long-lived process."""

import asyncio
from contextlib import asynccontextmanager

import nodriver as nd


@asynccontextmanager
async def open_browser(executable, profile_dir):
    # Retain the object even if startup fails, so its child can always be reaped.
    browser = nd.Browser(nd.Config(
        browser_executable_path=executable,
        user_data_dir=str(profile_dir), headless=False, sandbox=False,
        browser_args=["--disable-dev-shm-usage"],
    ))
    try:
        await browser.start()
        yield browser
    finally:
        process = browser._process
        browser.stop()  # aclose() alone closes only the websocket, not Chromium.
        try:
            if process is not None:
                try:
                    await asyncio.wait_for(process.communicate(), timeout=10)
                except TimeoutError:
                    process.kill()
                    await asyncio.wait_for(process.communicate(), timeout=5)
        finally:
            nd.util.get_registered_instances().discard(browser)
