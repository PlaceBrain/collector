from collections.abc import AsyncIterable
from urllib.parse import urlparse

import aiomqtt
from dishka import Provider, Scope, provide

from src.core.config import Settings


class MqttProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_mqtt_client(self, settings: Settings) -> AsyncIterable[aiomqtt.Client]:
        parsed = urlparse(str(settings.mqtt.url))
        client = aiomqtt.Client(
            hostname=parsed.hostname or "placebrain-emqx",
            port=parsed.port or 1883,
            username=settings.mqtt.username,
            password=settings.mqtt.password,
        )
        async with client:
            yield client
