# Collector Service

- **Port:** 50054
- **DB:** telemetry_db (TimescaleDB)
- Hybrid architecture: MQTT subscriber + gRPC server in a single asyncio event loop
- **Does not use SQLAlchemy/UoW/Repository** — raw asyncpg pool

## Structure

```
src/
├── main.py                          # gRPC server + MQTT loop, background tasks
├── core/
│   └── config.py                    # Pydantic Settings (App, Logging, MQTT, Database, Buffer)
├── dependencies/
│   ├── config.py                    # Settings (APP scope)
│   ├── db.py                        # asyncpg.Pool (APP scope)
│   ├── grpc.py                      # DevicesServiceStub (APP scope)
│   ├── mqtt.py                      # aiomqtt.Client (APP scope)
│   └── services.py                  # Buffer, Writer, ThresholdCache, AlertService, ReadingsService
├── handlers/
│   ├── readings.py                  # gRPC CollectorHandler (GetLatestReadings, GetReadings, DeleteReadings)
│   ├── telemetry.py                 # MQTT TelemetryHandler (buffer + thresholds)
│   └── status.py                    # MQTT StatusHandler (device status updates)
├── services/
│   ├── buffer.py                    # TelemetryBuffer (in-memory, async-safe)
│   ├── writer.py                    # TelemetryWriter (asyncpg COPY)
│   ├── readings.py                  # ReadingsService (raw + aggregated queries)
│   ├── threshold_cache.py           # ThresholdCache (refreshes every 5 min from devices)
│   └── alerts.py                    # AlertService (evaluate + MQTT publish + DB write)
└── infra/
    └── db.py                        # Schema creation (readings hypertable, alerts table)
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

- Refreshes every 5 min from devices service via gRPC `GetAllThresholds`
- On threshold violation → write to alerts + publish to MQTT `placebrain/{place_id}/alerts`

## gRPC

- `GetLatestReadings(device_id)` — `SELECT DISTINCT ON (key) ... ORDER BY time DESC`
- `GetReadings(device_id, from, to, interval_seconds, keys)` — raw (interval=0, max 2h) or aggregated (time_bucket_gapfill)
- `DeleteReadings(device_ids)` — cascading deletion of alerts + readings

## MQTT

- Username `collector`, password `collector` (hardcoded in devices service as trusted)
- Subscription: `placebrain/+/devices/+/telemetry`, `placebrain/+/devices/+/status`
- Depends on: postgres, MQTT broker, devices service (gRPC)
