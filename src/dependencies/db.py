import logging
from collections.abc import AsyncIterable

import asyncpg
from dishka import Provider, Scope, provide

from src.core.config import Settings
from src.infra.db import apply_schema, create_pool

logger = logging.getLogger(__name__)


class DBProvider(Provider):
    @provide(scope=Scope.APP)
    async def provide_pool(self, settings: Settings) -> AsyncIterable[asyncpg.Pool]:
        pool = await create_pool(str(settings.database.url), settings.database.pool_size)
        try:
            await apply_schema(pool)
        except asyncpg.PostgresError:
            logger.exception("Failed to apply schema")
        yield pool
        await pool.close()
