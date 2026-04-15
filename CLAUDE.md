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
    └── kafka/
        ├── consumers.py             # on_telemetry_reading, on_devices_event
        └── routes.py                # Kafka subscriber registration
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

**From `telemetry.readings`** (group: `collector-service`):
- Telemetry messages forwarded from EMQX Kafka bridge (topic, payload, timestamp)

**From `devices.events`** (group: `collector-service`):

| Event | Action |
|-------|--------|
| `ThresholdCreated` / `ThresholdUpdated` | Update threshold cache |
| `ThresholdDeleted` | Remove from threshold cache |
| `DevicesBulkDeleted` | Delete readings + cache for multiple devices |
| `DeviceDeleted` | Delete readings + cache for single device |

**Note:** `ReadingsService` is created once in `register_subscribers()` and reused across messages — not instantiated per message.

## Dependencies

- postgres, Kafka, Redis, EMQX (for alerts publishing)
