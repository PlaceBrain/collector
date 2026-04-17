import asyncio
import logging

import aiomqtt
import grpc
from dishka import make_async_container
from dishka.integrations.grpcio import DishkaAioInterceptor
from dishka_faststream import setup_dishka as setup_dishka_faststream
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
from src.infra.broker.routes import router as broker_router
from src.services.alerts import AlertService
from src.services.buffer import TelemetryBuffer
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
    logging.getLogger("aiokafka").setLevel(logging.WARNING)

    # gRPC server (for read endpoints)
    server = grpc.aio.server(interceptors=[DishkaAioInterceptor(container)])
    add_CollectorServiceServicer_to_server(CollectorHandler(), server)
    server.add_insecure_port(f"[::]:{settings.app.port}")
    await server.start()
    logger.info("Collector gRPC server started on port %s", settings.app.port)

    # Initialize services that need manual setup
    client = await container.get(aiomqtt.Client)
    buffer = await container.get(TelemetryBuffer)
    writer = await container.get(TelemetryWriter)
    alert_service = await container.get(AlertService)

    buffer.set_flush_callback(writer.write_batch)
    alert_service.set_client(client)

    # Kafka — include router, setup DI, then start
    broker = await container.get(KafkaBroker)
    broker.include_router(broker_router)
    setup_dishka_faststream(container, broker=broker, auto_inject=True)
    await broker.start()
    logger.info("Kafka consumers started")

    # Background tasks
    flush_task = asyncio.create_task(buffer.run_flush_loop())

    try:
        await server.wait_for_termination()
    finally:
        flush_task.cancel()
        await asyncio.gather(flush_task, return_exceptions=True)
        await broker.close()
        await server.stop(grace=3)
        await container.close()


if __name__ == "__main__":
    asyncio.run(serve())
