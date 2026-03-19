from collections.abc import AsyncIterable

import grpc
from dishka import Provider, Scope, provide
from placebrain_contracts.devices_pb2_grpc import DevicesServiceStub

from src.core.config import Settings


class GrpcProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_devices_stub(self, settings: Settings) -> AsyncIterable[DevicesServiceStub]:
        channel = grpc.aio.insecure_channel(settings.devices_service_url)
        try:
            yield DevicesServiceStub(channel)
        finally:
            await channel.close()
