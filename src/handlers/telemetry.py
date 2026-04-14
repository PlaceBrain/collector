import json
import logging
from datetime import UTC, datetime
from uuid import UUID

from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
from src.services.threshold_cache import ThresholdCache

logger = logging.getLogger(__name__)


class TelemetryHandler:
    def __init__(
        self,
        buffer: TelemetryBuffer,
        cache: ThresholdCache,
        alerts: AlertService,
    ) -> None:
        self._buffer = buffer
        self._cache = cache
        self._alerts = alerts

    async def handle(self, topic: str, payload: str) -> None:
        # Topic: placebrain/{place_id}/devices/{device_id}/telemetry
        parts = topic.split("/")
        if len(parts) < 5:
            logger.warning("Invalid telemetry topic: %s", topic)
            return

        device_id_str = parts[3]

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in telemetry message: %s", payload[:100])
            return

        try:
            device_id = UUID(device_id_str)
        except ValueError:
            logger.warning("Invalid device_id in topic: %s", device_id_str)
            return

        ts_raw = data.get("ts")
        if ts_raw is not None:
            try:
                ts = datetime.fromisoformat(ts_raw)
            except ValueError:
                ts = datetime.now(UTC)
        else:
            ts = datetime.now(UTC)

        values = data.get("values", {})
        if not isinstance(values, dict):
            logger.warning("Invalid values format in telemetry")
            return

        place_id = parts[1]

        for key, raw_value in values.items():
            try:
                value = float(raw_value)
            except TypeError, ValueError:
                logger.warning("Non-numeric value for key %s: %s", key, raw_value)
                continue

            await self._buffer.add(ts, device_id, key, value)

            mapping = self._cache.lookup(device_id_str, key)
            if mapping:
                await self._alerts.evaluate_and_alert(mapping, value, ts, place_id)
