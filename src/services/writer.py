import logging

import asyncpg

from src.services.buffer import TelemetryRecord

logger = logging.getLogger(__name__)


class TelemetryWriter:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def write_batch(self, records: list[TelemetryRecord]) -> None:
        if not records:
            return
        async with self._pool.acquire() as conn:
            await conn.copy_records_to_table(
                "readings",
                records=records,
                columns=["time", "device_id", "key", "value"],
            )
        logger.info("Flushed %d telemetry records", len(records))
