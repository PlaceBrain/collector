import json
import logging
from datetime import datetime
from uuid import UUID

import aiomqtt
import asyncpg

from src.services.threshold_cache import SensorMapping, ThresholdInfo

logger = logging.getLogger(__name__)


class AlertService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._client: aiomqtt.Client | None = None

    def set_client(self, client: aiomqtt.Client) -> None:
        self._client = client

    async def evaluate_and_alert(
        self,
        mapping: SensorMapping,
        value: float,
        timestamp: datetime,
        place_id: str,
    ) -> None:
        for threshold in mapping.thresholds:
            violated = (threshold.threshold_type == "max" and value > threshold.value) or (
                threshold.threshold_type == "min" and value < threshold.value
            )
            if violated:
                await self._create_alert(mapping, threshold, value, timestamp, place_id)

    async def _create_alert(
        self,
        mapping: SensorMapping,
        threshold: ThresholdInfo,
        value: float,
        timestamp: datetime,
        place_id: str,
    ) -> None:
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO alerts (
                        sensor_id, threshold_id, device_id, place_id,
                        key, value, threshold_value, threshold_type, severity
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id
                    """,
                    UUID(mapping.sensor_id),
                    UUID(threshold.threshold_id),
                    UUID(mapping.device_id),
                    UUID(place_id),
                    mapping.key,
                    value,
                    threshold.value,
                    threshold.threshold_type,
                    threshold.severity,
                )
        except asyncpg.PostgresError:
            logger.exception("Failed to write alert to DB")
            return

        alert_id = str(row["id"])
        alert_payload = {
            "id": alert_id,
            "event_type": "created",
            "sensor_id": mapping.sensor_id,
            "device_id": mapping.device_id,
            "place_id": place_id,
            "key": mapping.key,
            "value": value,
            "threshold_id": threshold.threshold_id,
            "threshold_type": threshold.threshold_type,
            "threshold_value": threshold.value,
            "severity": threshold.severity,
            "timestamp": timestamp.isoformat(),
        }

        topic = f"placebrain/{place_id}/alerts"
        if self._client:
            try:
                await self._client.publish(topic, json.dumps(alert_payload))
                logger.warning(
                    "Alert: %s %s=%s exceeds %s threshold %s (severity=%s)",
                    mapping.key,
                    mapping.device_id,
                    value,
                    threshold.threshold_type,
                    threshold.value,
                    threshold.severity,
                )
            except aiomqtt.MqttError:
                logger.exception("Failed to publish alert to MQTT")

    async def publish_resolved(
        self,
        *,
        alert_id: str,
        sensor_id: str,
        threshold_id: str,
        device_id: str,
        place_id: str,
        key: str,
        value: float,
        threshold_value: float,
        threshold_type: str,
        severity: str,
        resolved_at: datetime | None,
    ) -> None:
        payload = {
            "id": alert_id,
            "event_type": "resolved",
            "sensor_id": sensor_id,
            "device_id": device_id,
            "place_id": place_id,
            "key": key,
            "value": value,
            "threshold_id": threshold_id,
            "threshold_type": threshold_type,
            "threshold_value": threshold_value,
            "severity": severity,
            "resolved_at": resolved_at.isoformat() if resolved_at is not None else None,
        }
        topic = f"placebrain/{place_id}/alerts"
        if self._client:
            try:
                await self._client.publish(topic, json.dumps(payload))
            except aiomqtt.MqttError:
                logger.exception("Failed to publish resolved alert to MQTT")
