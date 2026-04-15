from dishka import Provider, Scope, provide
from faststream.kafka import KafkaBroker

from src.core.config import Settings


class KafkaProvider(Provider):
    @provide(scope=Scope.APP)
    def provide_broker(self, settings: Settings) -> KafkaBroker:
        return KafkaBroker(settings.kafka.url)
