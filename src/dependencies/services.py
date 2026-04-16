import asyncpg
from dishka import Provider, Scope, provide
from redis.asyncio import Redis

from src.core.config import Settings
from src.services.alerts import AlertService
from src.services.alerts_query import AlertsService
from src.services.buffer import TelemetryBuffer
from src.services.readings import ReadingsService
from src.services.threshold_cache import ThresholdCache
from src.services.writer import TelemetryWriter


class ServicesProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_buffer(self, settings: Settings) -> TelemetryBuffer:
        return TelemetryBuffer(settings.buffer.max_size, settings.buffer.flush_interval)

    @provide(scope=Scope.APP)
    def provide_writer(self, pool: asyncpg.Pool) -> TelemetryWriter:
        return TelemetryWriter(pool)

    @provide(scope=Scope.APP)
    def provide_threshold_cache(self, redis: Redis) -> ThresholdCache:
        return ThresholdCache(redis)

    @provide(scope=Scope.APP)
    def provide_alert_service(self, pool: asyncpg.Pool) -> AlertService:
        return AlertService(pool)

    @provide(scope=Scope.REQUEST)
    def provide_readings_service(self, pool: asyncpg.Pool) -> ReadingsService:
        return ReadingsService(pool)

    @provide(scope=Scope.REQUEST)
    def provide_alerts_service(self, pool: asyncpg.Pool) -> AlertsService:
        return AlertsService(pool)
