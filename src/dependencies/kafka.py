from collections.abc import AsyncIterable

from dishka import Provider, Scope, provide
from faststream.kafka import KafkaBroker

from src.core.config import Settings


class KafkaProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_broker(self, settings: Settings) -> AsyncIterable[KafkaBroker]:
        broker = KafkaBroker(settings.kafka.url)
        await broker.start()
        yield broker
        await broker.close()
