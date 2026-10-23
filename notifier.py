"""Telegram notifications for scan results."""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from pyrogram import Client

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self):
        self.api_id = os.environ.get("TELEGRAM_API_ID")
        self.api_hash = os.environ.get("TELEGRAM_API_HASH")
        self.phone = os.environ.get("TELEGRAM_PHONE")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        if not all([self.api_id, self.api_hash, self.phone, self.chat_id]):
            logger.warning("Telegram credentials not configured, notifications disabled")
            self.enabled = False
        else:
            self.enabled = True
            self.client = Client(
                name="scraper_bot",
                api_id=int(self.api_id),
                api_hash=self.api_hash,
                phone_number=self.phone,
                in_memory=True,  # Non salvare sessioni su disco
            )
            self._started = False

    async def start(self):
        """Start the Pyrogram client (call once at startup)."""
        if not self.enabled or self._started:
            return
        try:
            await self.client.start()
            self._started = True
            logger.info("Telegram client started")
        except Exception as exc:
            logger.error(f"Failed to start Telegram client: {exc}")
            self.enabled = False

    async def stop(self):
        """Stop the Pyrogram client (call at shutdown)."""
        if self.enabled and self._started:
            try:
                await self.client.stop()
                logger.info("Telegram client stopped")
            except Exception as exc:
                logger.warning(f"Error stopping Telegram client: {exc}")

    async def send_scan_success(self, total: int, added: int, changed: int, removed: int, duration: float):
        """Send notification for successful scan."""
        if not self.enabled:
            return

        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        message = (
            f"✅ **Scan riuscita** ({now})\n"
            f"📊 Totale: {total} annunci\n"
            f"➕ Nuovi: {added}\n"
            f"🔄 Modificati: {changed}\n"
            f"➖ Rimossi: {removed}\n"
            f"⏱️ Durata: {duration:.1f}s"
        )

        # Se non ci sono cambiamenti, usa un messaggio più breve
        if added == 0 and changed == 0 and removed == 0:
            message = f"✅ Scan OK ({now}): {total} annunci, nessuna novità ({duration:.1f}s)"

        await self._send(message)

    async def send_scan_failure(self, error_category: str, error_message: str, duration: float):
        """Send notification for failed scan."""
        if not self.enabled:
            return

        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        message = (
            f"❌ **Scan fallita** ({now})\n"
            f"🚫 Errore: {error_category}\n"
            f"💬 Dettaglio: {error_message[:200]}\n"
            f"⏱️ Durata: {duration:.1f}s"
        )
        await self._send(message)

    async def send_startup(self, interval: int):
        """Send notification when service starts."""
        if not self.enabled:
            return

        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        message = (
            f"🚀 **Servizio avviato** ({now})\n"
            f"⏰ Intervallo: {interval}s ({interval // 60} min)"
        )
        await self._send(message)

    async def _send(self, message: str):
        """Internal method to send message with error handling."""
        try:
            await self.client.send_message(int(self.chat_id), message)
            logger.info("Telegram notification sent")
        except Exception as exc:
            logger.error(f"Failed to send Telegram notification: {exc}")


# Singleton instance
notifier = TelegramNotifier()
