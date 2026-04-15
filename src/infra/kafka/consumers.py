import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
from src.services.readings import ReadingsService
from src.services.threshold_cache import ThresholdCache

logger = logging.getLogger(__name__)


async def on_telemetry_reading(
    data: dict[str, Any],
    buffer: TelemetryBuffer,
    cache: ThresholdCache,
    alerts: AlertService,
) -> None:
    """Handle telemetry messages forwarded from EMQX Kafka bridge."""
    # EMQX bridge sends: topic (MQTT topic), payload (raw), timestamp
    mqtt_topic = data.get("topic", "")
    raw_payload = data.get("payload", data)

    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in telemetry payload: %s", str(raw_payload)[:100])
            return
    elif isinstance(raw_payload, dict):
        payload = raw_payload
    else:
        logger.warning("Unexpected payload type: %s", type(raw_payload))
        return

    # Extract device_id from MQTT topic: placebrain/{place_id}/devices/{device_id}/telemetry
    parts = mqtt_topic.split("/") if mqtt_topic else []
    if len(parts) >= 5:
        device_id_str = parts[3]
        place_id = parts[1]
    else:
        device_id_str = payload.get("device_id", "")
        place_id = payload.get("place_id", "")

    if not device_id_str:
        logger.warning("Cannot extract device_id from telemetry message")
        return

    try:
        device_id = UUID(device_id_str)
    except ValueError:
        logger.warning("Invalid device_id: %s", device_id_str)
        return

    ts_raw = payload.get("ts")
    if ts_raw is not None:
        try:
            ts = datetime.fromisoformat(ts_raw)
        except ValueError:
            ts = datetime.now(UTC)
    else:
        ts = datetime.now(UTC)

    values = payload.get("values", {})
    if not isinstance(values, dict):
        return

    for key, raw_value in values.items():
        try:
            value = float(raw_value)
        except TypeError, ValueError:
            continue

        await buffer.add(ts, device_id, key, value)

        mapping = cache.lookup(device_id_str, key)
        if mapping:
            await alerts.evaluate_and_alert(mapping, value, ts, place_id)


async def on_devices_event(
    data: dict[str, Any],
    cache: ThresholdCache,
    readings_service: ReadingsService,
) -> None:
    """Handle device domain events (threshold changes, bulk deletes)."""
    event_type = data.get("event_type")

    if event_type == "threshold.created" or event_type == "threshold.updated":
        await cache.set_threshold(
            sensor_id=data["sensor_id"],
            threshold_id=data["threshold_id"],
            threshold_type=data["threshold_type"],
            value=data["value"],
            severity=data["severity"],
        )
        logger.info("Threshold cache updated: sensor=%s", data["sensor_id"])

    elif event_type == "threshold.deleted":
        await cache.remove_threshold(data["sensor_id"], data["threshold_id"])
        logger.info("Threshold removed from cache: %s", data["threshold_id"])

    elif event_type == "devices.bulk_deleted":
        device_ids = data["device_ids"]
        await readings_service.delete_readings(device_ids)
        await cache.delete_readings_for_devices(device_ids)
        logger.info("Deleted readings for %d devices", len(device_ids))

    elif event_type == "device.deleted":
        device_id = data["device_id"]
        await readings_service.delete_readings([device_id])
        await cache.delete_readings_for_devices([device_id])
        logger.info("Deleted readings for device %s", device_id)
