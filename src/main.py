import asyncio
import logging
from urllib.parse import urlparse

import aiomqtt
import grpc
from placebrain_contracts.devices_pb2_grpc import DevicesServiceStub

from src.core.config import Settings
from src.handlers.status import StatusHandler
from src.handlers.telemetry import TelemetryHandler
from src.infra.db import apply_schema, create_pool
from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
from src.services.threshold_cache import ThresholdCache
from src.services.writer import TelemetryWriter

logger = logging.getLogger(__name__)


async def serve() -> None:
    settings = Settings()

    logging.basicConfig(
        level=settings.logging.level.upper(),
        format=settings.logging.format,
        datefmt=settings.logging.date_format,
    )

    db_url = str(settings.database.url)
    pool = await create_pool(db_url, settings.database.pool_size)

    try:
        await apply_schema(pool)
    except Exception:
        logger.exception("Failed to apply schema (TimescaleDB may not be ready)")

    channel = grpc.aio.insecure_channel(settings.devices_service_url)
    devices_stub = DevicesServiceStub(channel)

    buffer = TelemetryBuffer(
        max_size=settings.buffer.max_size,
        flush_interval=settings.buffer.flush_interval,
    )
    writer = TelemetryWriter(pool)
    buffer.set_flush_callback(writer.write_batch)

    threshold_cache = ThresholdCache(devices_stub)
    alert_service = AlertService(pool)

    telemetry_handler = TelemetryHandler(buffer, threshold_cache, alert_service)
    status_handler = StatusHandler(devices_stub)

    flush_task = asyncio.create_task(buffer.run_flush_loop())
    cache_task = asyncio.create_task(threshold_cache.run_refresh_loop())

    parsed = urlparse(str(settings.mqtt.url))
    mqtt_host = parsed.hostname or "placebrain-emqx"
    mqtt_port = parsed.port or 1883

    logger.info("Collector service starting, connecting to MQTT at %s:%s", mqtt_host, mqtt_port)

    try:
        async with aiomqtt.Client(
            hostname=mqtt_host,
            port=mqtt_port,
            username=settings.mqtt.username,
            password=settings.mqtt.password,
        ) as client:
            alert_service.set_client(client)

            await client.subscribe("placebrain/+/devices/+/telemetry")
            await client.subscribe("placebrain/+/devices/+/status")
            logger.info("Subscribed to telemetry and status topics")

            async for message in client.messages:
                topic = str(message.topic)
                payload = message.payload
                if isinstance(payload, bytes):
                    payload = payload.decode()

                if topic.endswith("/telemetry"):
                    await telemetry_handler.handle(topic, payload)
                elif topic.endswith("/status"):
                    await status_handler.handle(topic, payload)
    finally:
        flush_task.cancel()
        cache_task.cancel()
        await pool.close()
        await channel.close()


if __name__ == "__main__":
    asyncio.run(serve())
