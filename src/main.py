import asyncio
import logging

import aiomqtt
import asyncpg
import grpc
from dishka import make_async_container
from dishka.integrations.grpcio import DishkaAioInterceptor
from faststream.kafka import KafkaBroker
from placebrain_contracts.collector_pb2_grpc import add_CollectorServiceServicer_to_server

from src.core.config import Settings
from src.dependencies.config import ConfigProvider
from src.dependencies.db import DBProvider
from src.dependencies.kafka import KafkaProvider
from src.dependencies.mqtt import MqttProvider
from src.dependencies.redis import RedisProvider
from src.dependencies.services import ServicesProvider
from src.handlers.readings import CollectorHandler
from src.infra.kafka.consumers import on_devices_event, on_telemetry_reading
from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
from src.services.readings import ReadingsService
from src.services.threshold_cache import ThresholdCache
from src.services.writer import TelemetryWriter

logger = logging.getLogger(__name__)


async def serve() -> None:
    container = make_async_container(
        ConfigProvider(),
        DBProvider(),
        RedisProvider(),
        KafkaProvider(),
        MqttProvider(),
        ServicesProvider(),
    )

    settings = await container.get(Settings)

    logging.basicConfig(
        level=settings.logging.level.upper(),
        format=settings.logging.format,
        datefmt=settings.logging.date_format,
    )

    # gRPC server (for read endpoints: GetLatestReadings, GetReadings, DeleteReadings)
    server = grpc.aio.server(interceptors=[DishkaAioInterceptor(container)])
    add_CollectorServiceServicer_to_server(CollectorHandler(), server)
    server.add_insecure_port(f"[::]:{settings.app.port}")
    await server.start()
    logger.info("Collector gRPC server started on port %s", settings.app.port)

    # Get dependencies
    broker = await container.get(KafkaBroker)
    client = await container.get(aiomqtt.Client)
    buffer = await container.get(TelemetryBuffer)
    writer = await container.get(TelemetryWriter)
    cache = await container.get(ThresholdCache)
    alert_service = await container.get(AlertService)

    buffer.set_flush_callback(writer.write_batch)
    alert_service.set_client(client)

    # Background tasks
    flush_task = asyncio.create_task(buffer.run_flush_loop())

    logger.info("Collector service starting Kafka consumers")

    # Kafka consumers
    async def consume_telemetry() -> None:
        async for msg in broker.subscriber("telemetry.readings", group_id="collector-service"):
            await on_telemetry_reading(msg, buffer, cache, alert_service)

    async def consume_devices_events() -> None:
        async for msg in broker.subscriber("devices.events", group_id="collector-service"):
            readings_service = ReadingsService(await container.get(asyncpg.Pool))
            await on_devices_event(msg, cache, readings_service)

    try:
        telemetry_task = asyncio.create_task(consume_telemetry())
        devices_task = asyncio.create_task(consume_devices_events())
        await server.wait_for_termination()
    finally:
        flush_task.cancel()
        telemetry_task.cancel()
        devices_task.cancel()
        await asyncio.gather(flush_task, telemetry_task, devices_task, return_exceptions=True)
        await server.stop(grace=3)
        await container.close()


if __name__ == "__main__":
    asyncio.run(serve())
