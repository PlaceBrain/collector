# Collector Service

- **Port:** 50054
- **DB:** telemetry_db (TimescaleDB)
- **Kafka:** consumer (`telemetry.readings`, `devices.events`)
- Hybrid architecture: Kafka subscriber + gRPC server in a single asyncio event loop
- **Does not use SQLAlchemy/UoW/Repository** — raw asyncpg pool

## Structure

```
src/
├── main.py                          # gRPC server + Kafka broker, background tasks
├── core/
│   └── config.py                    # Pydantic Settings (App, Logging, Database, Buffer, Kafka)
├── dependencies/
│   ├── config.py                    # Settings (APP scope)
│   ├── db.py                        # asyncpg.Pool (APP scope)
│   ├── kafka.py                     # KafkaBroker (APP scope)
│   └── services.py                  # Buffer, Writer, ThresholdCache, AlertService, ReadingsService
├── handlers/
│   └── readings.py                  # gRPC CollectorHandler (GetLatestReadings, GetReadings, DeleteReadings)
├── services/
│   ├── buffer.py                    # TelemetryBuffer (in-memory, async-safe)
│   ├── writer.py                    # TelemetryWriter (asyncpg COPY)
│   ├── readings.py                  # ReadingsService (raw + aggregated queries)
│   ├── threshold_cache.py           # ThresholdCache (Redis + local cache, updated via Kafka events)
│   └── alerts.py                    # AlertService (evaluate + MQTT publish + DB write)
└── infra/
    ├── db.py                        # Schema creation (readings hypertable, alerts table)
    └── broker/
        └── routes.py                # KafkaRouter with typed subscribers (FromDishka DI)
```

## Protobuf Imports

```python
from placebrain_contracts import collector_pb2 as collector_pb
from placebrain_contracts import devices_pb2 as devices_pb
```

## Tables

- `readings` (hypertable: time, device_id, key, value) — TimescaleDB with compression and retention
- `alerts` (sensor_id, threshold_id, device_id, place_id, key, value, threshold_value, threshold_type, severity)

## Buffering

- In-memory buffer, up to 1000 records or 60 sec
- Flush via `asyncpg COPY`

## Threshold Cache

- Updated in real-time via Kafka events (`ThresholdCreated`, `ThresholdUpdated`, `ThresholdDeleted`) from `devices.events` topic
- Two-tier: Redis (persistent) + local dict (fast lookup per telemetry message)
- On threshold violation → write to alerts + publish to MQTT `placebrain/{place_id}/alerts`

## gRPC

- `GetLatestReadings(device_id)` — `SELECT DISTINCT ON (key) ... ORDER BY time DESC`
- `GetReadings(device_id, from, to, interval_seconds, keys)` — raw (interval=0, max 2h) or aggregated (time_bucket_gapfill)
- `DeleteReadings(device_ids)` — cascading deletion of alerts + readings

## Kafka Events (Consumer)

Subscribers in `src/infra/broker/routes.py` via `KafkaRouter` + `FromDishka[]`. One topic per event type.

**Consumes** (group: `collector-service`):

| Topic | Event | Action |
|-------|-------|--------|
| `telemetry.readings` | `EmqxTelemetryMessage` | Buffer telemetry, evaluate thresholds, alert |
| `devices.threshold.created` | `ThresholdCreated` | Update threshold cache |
| `devices.threshold.deleted` | `ThresholdDeleted` | Remove from threshold cache |
| `devices.device.bulk-deleted` | `DevicesBulkDeleted` | Delete readings + cache for multiple devices |
| `devices.device.deleted` | `DeviceDeleted` | Delete readings + cache for single device |

## Dependencies

- postgres, Kafka, Redis, EMQX (for alerts publishing)
