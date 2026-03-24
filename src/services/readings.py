from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

import asyncpg

MAX_ROWS = 50_000
MAX_RAW_RANGE_HOURS = 2


@dataclass
class SensorReading:
    key: str
    value: float
    time: datetime


@dataclass
class AggregatedReading:
    time: datetime
    avg: float | None
    min: float | None
    max: float | None


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

    async def get_readings_raw(
        self,
        device_id: str,
        keys: list[str],
        time_from: datetime,
        time_to: datetime,
    ) -> dict[str, list[SensorReading]]:
        has_keys = len(keys) > 0
        query = f"""
            SELECT key, value, time FROM readings
            WHERE device_id = $1::uuid
              {"AND key = ANY($4::text[])" if has_keys else ""}
              AND time >= $2 AND time < $3
            ORDER BY key, time ASC
            LIMIT {MAX_ROWS}
        """
        params: list = [UUID(device_id), time_from, time_to]
        if has_keys:
            params.append(keys)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        result: dict[str, list[SensorReading]] = {}
        for r in rows:
            result.setdefault(r["key"], []).append(
                SensorReading(key=r["key"], value=r["value"], time=r["time"])
            )
        return result

    async def get_readings_aggregated(
        self,
        device_id: str,
        keys: list[str],
        time_from: datetime,
        time_to: datetime,
        interval_seconds: int,
    ) -> dict[str, list[AggregatedReading]]:
        has_keys = len(keys) > 0
        interval = timedelta(seconds=interval_seconds)
        query = f"""
            SELECT
                time_bucket_gapfill($1::interval, time) AS bucket,
                key,
                avg(value) AS avg_val,
                min(value) AS min_val,
                max(value) AS max_val
            FROM readings
            WHERE device_id = $2::uuid
              {"AND key = ANY($5::text[])" if has_keys else ""}
              AND time >= $3 AND time < $4
            GROUP BY bucket, key
            ORDER BY key, bucket ASC
            LIMIT {MAX_ROWS}
        """
        params: list = [interval, UUID(device_id), time_from, time_to]
        if has_keys:
            params.append(keys)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        result: dict[str, list[AggregatedReading]] = {}
        for r in rows:
            result.setdefault(r["key"], []).append(
                AggregatedReading(
                    time=r["bucket"],
                    avg=r["avg_val"],
                    min=r["min_val"],
                    max=r["max_val"],
                )
            )
        return result
