import logging
from typing import Any

import asyncpg
from faststream.kafka import KafkaBroker

from src.infra.kafka.consumers import on_devices_event, on_telemetry_reading
from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
from src.services.readings import ReadingsService
from src.services.threshold_cache import ThresholdCache

logger = logging.getLogger(__name__)


def register_subscribers(
    broker: KafkaBroker,
    buffer: TelemetryBuffer,
    cache: ThresholdCache,
    alert_service: AlertService,
    pool: asyncpg.Pool,
) -> None:
    readings_service = ReadingsService(pool)

    @broker.subscriber("telemetry.readings", group_id="collector-service", no_ack=True)
    async def handle_telemetry(msg: dict[str, Any]) -> None:
        try:
            await on_telemetry_reading(msg, buffer, cache, alert_service)
        except Exception:
            logger.exception("Failed to handle telemetry reading")

    @broker.subscriber("devices.events", group_id="collector-service", no_ack=True)
    async def handle_devices_event(msg: dict[str, Any]) -> None:
        try:
            await on_devices_event(msg, cache, readings_service)
        except Exception:
            logger.exception("Failed to handle devices event: %s", msg.get("event_type"))
