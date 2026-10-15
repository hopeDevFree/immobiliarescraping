"""Own and reap Chromium across repeated scans in a long-lived process."""

import asyncio
import os
from contextlib import asynccontextmanager

import nodriver as nd


async def start_browser(browser, timeout=30):
    """Allow cold Chromium startup on a small server without respawning it."""
    deadline = asyncio.get_running_loop().time() + timeout
    try:
        await browser.start()
        return
    except Exception as original:
        process = browser._process
        if process is None or process.returncode is not None or "Failed to connect to browser" not in str(original):
            raise
        while asyncio.get_running_loop().time() < deadline:
            if process.returncode is not None:
                raise RuntimeError("Chromium exited while waiting for its debugger") from original
            try:
                info = await asyncio.wait_for(browser._http.get("version"), timeout=2)
            except (OSError, TimeoutError):
                await asyncio.sleep(0.5)
                continue
            browser.info = nd.ContraDict(info, silent=True)
            browser.websocket_url = info["webSocketDebuggerUrl"]
            await browser.attach()
            await browser.update_targets()
            return
        raise RuntimeError(f"Chromium debugger was not ready within {timeout} seconds") from original


@asynccontextmanager
async def open_browser(executable, profile_dir):
    # Retain the object even if startup fails, so its child can always be reaped.
    browser = nd.Browser(nd.Config(
        browser_executable_path=executable,
        user_data_dir=str(profile_dir), headless=False, sandbox=False,
        browser_args=["--disable-dev-shm-usage"],
    ))
    startup_failure = None
    try:
        try:
            await start_browser(browser)
        except Exception as exc:
            startup_failure = exc
            raise
        yield browser
    finally:
        process = browser._process
        exit_before_cleanup = process.returncode if process is not None else None
        stderr = b""
        browser.stop()  # aclose() alone closes only the websocket, not Chromium.
        try:
            if process is not None:
                try:
                    output = await asyncio.wait_for(process.communicate(), timeout=10)
                except TimeoutError:
                    if process.returncode is None:
                        process.kill()
                    output = await asyncio.wait_for(process.communicate(), timeout=5)
                if isinstance(output, tuple):
                    stderr = output[1] or b""
        finally:
            nd.util.get_registered_instances().discard(browser)
        if startup_failure is not None:
            detail = stderr.decode("utf-8", errors="replace")[-2000:]
            raise RuntimeError(
                f"Chromium startup failed: DISPLAY={os.environ.get('DISPLAY', '<unset>')}; "
                f"exit_before_cleanup={exit_before_cleanup}; stderr={detail or '<empty>'}; "
                f"cause={startup_failure}"
            ) from startup_failure
