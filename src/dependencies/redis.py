from collections.abc import AsyncIterable

from dishka import Provider, Scope, provide
from redis.asyncio import Redis

from src.core.config import Settings


class RedisProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_redis(self, settings: Settings) -> AsyncIterable[Redis]:
        redis = Redis.from_url(settings.redis.url, decode_responses=False)
        yield redis
        await redis.aclose()
