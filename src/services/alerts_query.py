from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


@dataclass
class AlertRow:
    id: str
    sensor_id: str
    threshold_id: str
    device_id: str
    place_id: str
    key: str
    value: float
    threshold_value: float
    threshold_type: str
    severity: str
    status: str
    created_at: datetime
    resolved_at: datetime | None


def _row_to_alert(row: asyncpg.Record) -> AlertRow:
    return AlertRow(
        id=str(row["id"]),
        sensor_id=str(row["sensor_id"]),
        threshold_id=str(row["threshold_id"]),
        device_id=str(row["device_id"]),
        place_id=str(row["place_id"]),
        key=row["key"],
        value=row["value"],
        threshold_value=row["threshold_value"],
        threshold_type=row["threshold_type"],
        severity=row["severity"],
        status=row["status"],
        created_at=row["created_at"],
        resolved_at=row["resolved_at"],
    )


class AlertsService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def list_alerts(
        self,
        place_id: str,
        status: str | None,
        severity: str | None,
        sensor_id: str | None,
        device_id: str | None,
        time_from: datetime | None,
        time_to: datetime | None,
        page: int,
        per_page: int,
    ) -> tuple[list[AlertRow], int]:
        if per_page <= 0:
            per_page = DEFAULT_PAGE_SIZE
        per_page = min(per_page, MAX_PAGE_SIZE)
        if page <= 0:
            page = 1

        conditions: list[str] = ["place_id = $1::uuid"]
        params: list[Any] = [UUID(place_id)]

        if status:
            params.append(status)
            conditions.append(f"status = ${len(params)}")
        if severity:
            params.append(severity)
            conditions.append(f"severity = ${len(params)}")
        if sensor_id:
            params.append(UUID(sensor_id))
            conditions.append(f"sensor_id = ${len(params)}::uuid")
        if device_id:
            params.append(UUID(device_id))
            conditions.append(f"device_id = ${len(params)}::uuid")
        if time_from is not None:
            params.append(time_from)
            conditions.append(f"created_at >= ${len(params)}")
        if time_to is not None:
            params.append(time_to)
            conditions.append(f"created_at < ${len(params)}")

        where = " AND ".join(conditions)
        offset = (page - 1) * per_page

        list_query = f"""
            SELECT id, sensor_id, threshold_id, device_id, place_id,
                   key, value, threshold_value, threshold_type, severity,
                   status, created_at, resolved_at
            FROM alerts
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT {per_page} OFFSET {offset}
        """
        count_query = f"SELECT COUNT(*) AS total FROM alerts WHERE {where}"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(list_query, *params)
            total_row = await conn.fetchrow(count_query, *params)

        items = [_row_to_alert(r) for r in rows]
        total = int(total_row["total"]) if total_row else 0
        return items, total

    async def count_unresolved(self, place_ids: list[str]) -> tuple[dict[str, int], int]:
        if not place_ids:
            return {}, 0
        uuids = [UUID(p) for p in place_ids]
        query = """
            SELECT place_id, COUNT(*) AS cnt
            FROM alerts
            WHERE status = 'active' AND place_id = ANY($1::uuid[])
            GROUP BY place_id
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, uuids)

        per_place: dict[str, int] = {str(r["place_id"]): int(r["cnt"]) for r in rows}
        total = sum(per_place.values())
        return per_place, total

    async def resolve(self, alert_id: str) -> AlertRow | None:
        query = """
            UPDATE alerts
            SET status = 'resolved', resolved_at = NOW()
            WHERE id = $1::uuid AND status != 'resolved'
            RETURNING id, sensor_id, threshold_id, device_id, place_id,
                      key, value, threshold_value, threshold_type, severity,
                      status, created_at, resolved_at
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, UUID(alert_id))
        if row is None:
            return None
        return _row_to_alert(row)
