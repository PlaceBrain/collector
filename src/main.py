import asyncio
import logging

import aiomqtt
import grpc
from dishka import make_async_container
from dishka.integrations.grpcio import DishkaAioInterceptor
from placebrain_contracts.collector_pb2_grpc import add_CollectorServiceServicer_to_server
from placebrain_contracts.devices_pb2_grpc import DevicesServiceStub

from src.core.config import Settings
from src.dependencies.config import ConfigProvider
from src.dependencies.db import DBProvider
from src.dependencies.grpc import GrpcProvider
from src.dependencies.mqtt import MqttProvider
from src.dependencies.services import ServicesProvider
from src.handlers.readings import CollectorHandler
from src.handlers.status import StatusHandler
from src.handlers.telemetry import TelemetryHandler
from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
from src.services.threshold_cache import ThresholdCache
from src.services.writer import TelemetryWriter

logger = logging.getLogger(__name__)


async def serve() -> None:
    container = make_async_container(
        ConfigProvider(),
        DBProvider(),
        GrpcProvider(),
        MqttProvider(),
        ServicesProvider(),
    )

    settings = await container.get(Settings)

    logging.basicConfig(
        level=settings.logging.level.upper(),
        format=settings.logging.format,
        datefmt=settings.logging.date_format,
    )

    # gRPC server
    server = grpc.aio.server(interceptors=[DishkaAioInterceptor(container)])
    add_CollectorServiceServicer_to_server(CollectorHandler(), server)
    server.add_insecure_port(f"[::]:{settings.app.port}")
    await server.start()
    logger.info("Collector gRPC server started on port %s", settings.app.port)

    # Get MQTT-related deps from container
    client = await container.get(aiomqtt.Client)
    buffer = await container.get(TelemetryBuffer)
    writer = await container.get(TelemetryWriter)
    threshold_cache = await container.get(ThresholdCache)
    alert_service = await container.get(AlertService)
    devices_stub = await container.get(DevicesServiceStub)

    buffer.set_flush_callback(writer.write_batch)
    alert_service.set_client(client)

    telemetry_handler = TelemetryHandler(buffer, threshold_cache, alert_service)
    status_handler = StatusHandler(devices_stub)

    # Background tasks
    flush_task = asyncio.create_task(buffer.run_flush_loop())
    cache_task = asyncio.create_task(threshold_cache.run_refresh_loop())

    logger.info("Collector service starting, subscribing to MQTT topics")

    try:
        await client.subscribe("placebrain/+/devices/+/telemetry")
        await client.subscribe("placebrain/+/devices/+/status")
        logger.info("Subscribed to telemetry and status topics")

        async for message in client.messages:
            topic = str(message.topic)
            raw_payload = message.payload
            payload = raw_payload.decode() if isinstance(raw_payload, bytes) else str(raw_payload)

            if topic.endswith("/telemetry"):
                await telemetry_handler.handle(topic, payload)
            elif topic.endswith("/status"):
                await status_handler.handle(topic, payload)
    finally:
        flush_task.cancel()
        cache_task.cancel()
        await asyncio.gather(flush_task, cache_task, return_exceptions=True)
        await server.stop(grace=3)
        await container.close()


if __name__ == "__main__":
    asyncio.run(serve())
