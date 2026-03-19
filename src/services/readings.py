from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg


@dataclass
class SensorReading:
    key: str
    value: float
    time: datetime


class ReadingsService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def get_latest(self, device_id: str) -> list[SensorReading]:
        query = """
            SELECT DISTINCT ON (key) key, value, time
            FROM readings
            WHERE device_id = $1::uuid
            ORDER BY key, time DESC
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, UUID(device_id))
        return [SensorReading(key=r["key"], value=r["value"], time=r["time"]) for r in rows]
