import json
import logging
from datetime import UTC, datetime
from uuid import UUID

import aiomqtt
import asyncpg

from src.services.alerts_query import AlertRow
from src.services.threshold_cache import SensorMapping, ThresholdInfo

logger = logging.getLogger(__name__)


def _alert_payload(row: AlertRow, event_type: str) -> dict[str, object]:
    return {
        "id": row.id,
        "event_type": event_type,
        "sensor_id": row.sensor_id,
        "threshold_id": row.threshold_id,
        "device_id": row.device_id,
        "place_id": row.place_id,
        "key": row.key,
        "value": row.value,
        "threshold_value": row.threshold_value,
        "threshold_type": row.threshold_type,
        "severity": row.severity,
        "created_at": row.created_at.isoformat(),
        "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
    }


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
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO alerts (
                        sensor_id, threshold_id, device_id, place_id,
                        key, value, threshold_value, threshold_type, severity
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id, created_at
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

        row = AlertRow(
            id=str(inserted["id"]),
            sensor_id=mapping.sensor_id,
            threshold_id=threshold.threshold_id,
            device_id=mapping.device_id,
            place_id=place_id,
            key=mapping.key,
            value=value,
            threshold_value=threshold.value,
            threshold_type=threshold.threshold_type,
            severity=threshold.severity,
            status="active",
            created_at=inserted["created_at"] or timestamp.astimezone(UTC),
            resolved_at=None,
        )
        await self._publish(row, "created")
        logger.warning(
            "Alert: %s %s=%s exceeds %s threshold %s (severity=%s)",
            row.key,
            row.device_id,
            row.value,
            row.threshold_type,
            row.threshold_value,
            row.severity,
        )

    async def publish_resolved(self, row: AlertRow) -> None:
        await self._publish(row, "resolved")

    async def _publish(self, row: AlertRow, event_type: str) -> None:
        if not self._client:
            return
        topic = f"placebrain/{row.place_id}/alerts"
        try:
            await self._client.publish(topic, json.dumps(_alert_payload(row, event_type)))
        except aiomqtt.MqttError:
            logger.exception("Failed to publish %s alert to MQTT", event_type)
