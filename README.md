# collector

> Telemetry ingestion and threshold evaluation for PlaceBrain — Kafka in, TimescaleDB out, MQTT alerts sideways.

[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-hypertable-orange.svg)
![Kafka](https://img.shields.io/badge/Kafka-4.0-black.svg)

The hot path of the platform. Every device message landing in EMQX is bridged to Kafka; this service consumes it, batches it in memory, evaluates configured thresholds, writes rows to a TimescaleDB hypertable with `COPY`, and publishes alerts back to MQTT when something misbehaves. A small gRPC surface serves aggregated readings back to the rest of the system.

## Role in PlaceBrain

PlaceBrain is an open-source IoT platform for smart buildings. See the [organization profile](https://github.com/PlaceBrain) for the full architecture.

- Consumes `telemetry.readings` (EMQX → Kafka bridge) and `devices.*` (from [devices](https://github.com/PlaceBrain/devices)) for threshold cache updates and cascading deletes.
- Writes `readings` (hypertable) and `alerts` into `telemetry_db`.
- Publishes `placebrain/{place_id}/alerts` on MQTT when a threshold is crossed.
- Exposed over gRPC (port 50054) for the [gateway](https://github.com/PlaceBrain/gateway) to fetch latest/aggregated readings.

## Tech stack

- Python 3.14, uv
- gRPC + FastStream on aiokafka, `dishka-faststream` for DI in subscribers
- [aiomqtt](https://github.com/sbtinstruments/aiomqtt) for alert publishing
- **Raw asyncpg pool** — no SQLAlchemy, no Repository pattern. The hot path uses `COPY` directly.
- [TimescaleDB](https://www.timescale.com/) — `readings` is a hypertable with compression and retention policies
- Redis + in-memory dict for the two-tier threshold cache

## gRPC methods (port 50054)

All internal (no auth, Docker-network only):

| Method | Purpose |
|---|---|
| `GetLatestReadings(device_id)` | `SELECT DISTINCT ON (key) ... ORDER BY time DESC` — latest value per sensor key |
| `GetReadings(device_id, from, to, interval_seconds, keys)` | Raw rows when `interval_seconds=0` (max 2h), otherwise `time_bucket_gapfill` aggregation |
| `DeleteReadings(device_ids)` | Cascade: delete alerts, then readings |

Proto definitions in [placebrain-contracts](https://github.com/PlaceBrain/contracts) (`collector.proto`).

## Kafka events (consumer)

Consumer group `collector-service`:

| Topic | Event | Action |
|---|---|---|
| `telemetry.readings` | `EmqxTelemetryMessage` | Buffer → COPY into `readings`, evaluate thresholds, alert |
| `devices.threshold.created` | `ThresholdCreated` | Update threshold cache |
| `devices.threshold.deleted` | `ThresholdDeleted` | Drop from cache |
| `devices.device.deleted` | `DeviceDeleted` | Cascade delete readings + alerts for one device |
| `devices.device.bulk-deleted` | `DevicesBulkDeleted` | Same, but batched (e.g. when a place is deleted) |

## Buffering model

- `TelemetryBuffer` — in-memory, async-safe (`asyncio.Lock`), bounded by `BUFFER__MAX_SIZE` (default 1000).
- `TelemetryWriter` flushes via `asyncpg.copy_records_to_table` on the `BUFFER__FLUSH_INTERVAL` cadence (default 60s) or whenever the buffer is full.
- `ThresholdCache` — Redis is the persistent source of truth; a local `dict` is checked on every incoming message to avoid Redis round-trips.

## Local development

**Full stack (recommended):** clone [infra](https://github.com/PlaceBrain/infra) and run `make dev`. The compose stack builds the custom Postgres+TimescaleDB image and runs the init script that creates `telemetry_db` and enables the extension.

**Service-only mode:**

```bash
uv sync
cp .env.example .env          # set DATABASE__URL (TimescaleDB), MQTT__*, KAFKA__URL, REDIS__URL
uv run python -m src
```

No migrations — schema is created on startup from `src/infra/db.py`. You need a TimescaleDB instance with `CREATE EXTENSION timescaledb` available.

## Environment variables

See [`.env.example`](./.env.example).

## Project layout

```
src/
├── main.py
├── core/config.py           Pydantic Settings
├── dependencies/            Dishka providers (config, db pool, kafka, mqtt, redis, services)
├── handlers/readings.py     gRPC CollectorHandler
├── services/
│   ├── buffer.py            In-memory async-safe telemetry buffer
│   ├── writer.py            asyncpg COPY writer
│   ├── readings.py          Raw + time_bucket_gapfill queries
│   ├── threshold_cache.py   Redis + in-memory cache
│   └── alerts.py            Evaluate thresholds, write to `alerts`, publish to MQTT
└── infra/
    ├── db.py                Schema / hypertable / compression / retention setup
    └── broker/routes.py     KafkaRouter subscribers
```

## License

Apache License 2.0 — see [LICENSE](./LICENSE).
