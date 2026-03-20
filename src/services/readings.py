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

    async def delete_readings(self, device_ids: list[str]) -> None:
        uuids = [UUID(did) for did in device_ids]
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM alerts WHERE device_id = ANY($1::uuid[])", uuids)
            await conn.execute("DELETE FROM readings WHERE device_id = ANY($1::uuid[])", uuids)

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
