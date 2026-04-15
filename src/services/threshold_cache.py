import logging
from dataclasses import dataclass
from typing import Any

import orjson
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


@dataclass
class ThresholdInfo:
    threshold_id: str
    sensor_id: str
    threshold_type: str  # "min" or "max"
    value: float
    severity: str  # "warning" or "critical"


@dataclass
class SensorMapping:
    sensor_id: str
    device_id: str
    place_id: str
    key: str
    thresholds: list[ThresholdInfo]


class ThresholdCache:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._local_cache: dict[tuple[str, str], SensorMapping] = {}

    def lookup(self, device_id: str, key: str) -> SensorMapping | None:
        return self._local_cache.get((device_id, key))

    async def set_threshold(
        self,
        sensor_id: str,
        threshold_id: str,
        threshold_type: str,
        value: float,
        severity: str,
        device_id: str = "",
        key: str = "",
    ) -> None:
        redis_key = f"thresholds:{sensor_id}"
        raw = await self._redis.get(redis_key)
        thresholds: list[dict[str, Any]] = orjson.loads(raw) if raw else []

        updated = False
        for t in thresholds:
            if t["threshold_id"] == threshold_id:
                t["threshold_type"] = threshold_type
                t["value"] = value
                t["severity"] = severity
                updated = True
                break
        if not updated:
            thresholds.append(
                {
                    "threshold_id": threshold_id,
                    "sensor_id": sensor_id,
                    "threshold_type": threshold_type,
                    "value": value,
                    "severity": severity,
                }
            )
        await self._redis.set(redis_key, orjson.dumps(thresholds))

        if device_id and key:
            self._update_local_cache(device_id, key, sensor_id, thresholds)

    async def remove_threshold(self, sensor_id: str, threshold_id: str) -> None:
        redis_key = f"thresholds:{sensor_id}"
        raw = await self._redis.get(redis_key)
        if not raw:
            return
        thresholds: list[dict[str, Any]] = orjson.loads(raw)
        thresholds = [t for t in thresholds if t["threshold_id"] != threshold_id]
        if thresholds:
            await self._redis.set(redis_key, orjson.dumps(thresholds))
        else:
            await self._redis.delete(redis_key)

    async def delete_readings_for_devices(self, device_ids: list[str]) -> None:
        """Clean up threshold cache entries for deleted devices."""
        keys_to_remove = [
            cache_key for cache_key in self._local_cache if cache_key[0] in device_ids
        ]
        for cache_key in keys_to_remove:
            del self._local_cache[cache_key]

    def _update_local_cache(
        self, device_id: str, key: str, sensor_id: str, thresholds: list[dict[str, Any]]
    ) -> None:
        self._local_cache[(device_id, key)] = SensorMapping(
            sensor_id=sensor_id,
            device_id=device_id,
            place_id="",
            key=key,
            thresholds=[
                ThresholdInfo(
                    threshold_id=t["threshold_id"],
                    sensor_id=t["sensor_id"],
                    threshold_type=t["threshold_type"],
                    value=t["value"],
                    severity=t["severity"],
                )
                for t in thresholds
            ],
        )
