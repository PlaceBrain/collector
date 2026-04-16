import logging
from datetime import UTC, datetime
from uuid import UUID

from dishka_faststream import FromDishka
from faststream.kafka import KafkaRouter
from placebrain_contracts.events import (
    TOPIC_DEVICE_DELETED,
    TOPIC_DEVICES_BULK_DELETED,
    TOPIC_TELEMETRY_READINGS,
    TOPIC_THRESHOLD_CREATED,
    TOPIC_THRESHOLD_DELETED,
    DeviceDeleted,
    DevicesBulkDeleted,
    EmqxTelemetryMessage,
    ThresholdCreated,
    ThresholdDeleted,
)

from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
from src.services.readings import ReadingsService
from src.services.threshold_cache import ThresholdCache

logger = logging.getLogger(__name__)
router = KafkaRouter()


@router.subscriber(TOPIC_TELEMETRY_READINGS, group_id="collector-service")
async def on_telemetry_reading(
    msg: EmqxTelemetryMessage,
    buffer: FromDishka[TelemetryBuffer],
    cache: FromDishka[ThresholdCache],
    alerts: FromDishka[AlertService],
) -> None:
    place_id, device_id_str = msg.extract_ids()
    try:
        device_id = UUID(device_id_str)
    except ValueError:
        logger.warning("Invalid device_id: %s", device_id_str)
        return

    ts = msg.payload.ts or datetime.now(UTC)

    for key, value in msg.payload.values.items():
        await buffer.add(ts, device_id, key, value)
        mapping = cache.lookup(device_id_str, key)
        if mapping:
            await alerts.evaluate_and_alert(mapping, value, ts, place_id)


@router.subscriber(TOPIC_THRESHOLD_CREATED, group_id="collector-service")
async def on_threshold_created(
    event: ThresholdCreated,
    cache: FromDishka[ThresholdCache],
) -> None:
    await cache.set_threshold(
        sensor_id=str(event.sensor_id),
        threshold_id=str(event.threshold_id),
        threshold_type=event.threshold_type,
        value=event.value,
        severity=event.severity,
    )
    logger.info("Threshold cache updated: sensor=%s", event.sensor_id)


@router.subscriber(TOPIC_THRESHOLD_DELETED, group_id="collector-service")
async def on_threshold_deleted(
    event: ThresholdDeleted,
    cache: FromDishka[ThresholdCache],
) -> None:
    await cache.remove_threshold(str(event.sensor_id), str(event.threshold_id))
    logger.info("Threshold removed from cache: %s", event.threshold_id)


@router.subscriber(TOPIC_DEVICES_BULK_DELETED, group_id="collector-service")
async def on_devices_bulk_deleted(
    event: DevicesBulkDeleted,
    cache: FromDishka[ThresholdCache],
    readings: FromDishka[ReadingsService],
) -> None:
    device_ids = [str(d) for d in event.device_ids]
    await readings.delete_readings(device_ids)
    await cache.delete_readings_for_devices(device_ids)
    logger.info("Deleted readings for %d devices", len(device_ids))


@router.subscriber(TOPIC_DEVICE_DELETED, group_id="collector-service")
async def on_device_deleted(
    event: DeviceDeleted,
    cache: FromDishka[ThresholdCache],
    readings: FromDishka[ReadingsService],
) -> None:
    device_id = str(event.device_id)
    await readings.delete_readings([device_id])
    await cache.delete_readings_for_devices([device_id])
    logger.info("Deleted readings for device %s", event.device_id)
