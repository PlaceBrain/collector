import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from uuid import UUID

logger = logging.getLogger(__name__)

type TelemetryRecord = tuple[datetime, UUID, str, float]
type FlushCallback = Callable[[list[TelemetryRecord]], Awaitable[None]]


class TelemetryBuffer:
    def __init__(self, max_size: int = 1000, flush_interval: int = 60) -> None:
        self._buffer: list[TelemetryRecord] = []
        self._max_size = max_size
        self._flush_interval = flush_interval
        self._last_flush = time.monotonic()
        self._lock = asyncio.Lock()
        self._flush_callback: FlushCallback | None = None

    def set_flush_callback(self, callback: FlushCallback) -> None:
        self._flush_callback = callback

    async def add(self, ts: datetime, device_id: UUID, key: str, value: float) -> None:
        async with self._lock:
            self._buffer.append((ts, device_id, key, value))

    def should_flush(self) -> bool:
        if len(self._buffer) >= self._max_size:
            return True
        if time.monotonic() - self._last_flush >= self._flush_interval:
            return True
        return False

    async def drain(self) -> list[TelemetryRecord]:
        async with self._lock:
            records = self._buffer.copy()
            self._buffer.clear()
            self._last_flush = time.monotonic()
            return records

    async def run_flush_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            if self.should_flush() and self._buffer:
                records = await self.drain()
                if records and self._flush_callback is not None:
                    try:
                        await self._flush_callback(records)
                    except Exception:
                        logger.exception("Failed to flush %d records", len(records))
