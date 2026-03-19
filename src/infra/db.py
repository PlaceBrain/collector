import logging

import asyncpg

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS readings (
    time        TIMESTAMPTZ NOT NULL,
    device_id   UUID NOT NULL,
    key         TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('readings', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_readings_device_key_time
    ON readings (device_id, key, time DESC);

CREATE TABLE IF NOT EXISTS alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sensor_id       UUID NOT NULL,
    threshold_id    UUID NOT NULL,
    device_id       UUID NOT NULL,
    place_id        UUID NOT NULL,
    key             TEXT NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    threshold_type  TEXT NOT NULL,
    severity        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (status) WHERE status = 'active';
"""


async def create_pool(dsn: str, pool_size: int) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=pool_size)
    assert pool is not None
    logger.info("Database pool created")
    return pool


async def apply_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)
    logger.info("Schema applied successfully")
