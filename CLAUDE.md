# Collector Service

- **Порт:** 50054
- **БД:** telemetry_db (TimescaleDB)
- Гибридная архитектура: MQTT-подписчик + gRPC-сервер в одном asyncio event loop
- **Не использует SQLAlchemy/UoW/Repository** — raw asyncpg pool

## Структура

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
│   ├── telemetry.py                 # MQTT TelemetryHandler (буфер + пороги)
│   └── status.py                    # MQTT StatusHandler (обновление статуса устройств)
├── services/
│   ├── buffer.py                    # TelemetryBuffer (in-memory, async-safe)
│   ├── writer.py                    # TelemetryWriter (asyncpg COPY)
│   ├── readings.py                  # ReadingsService (raw + aggregated queries)
│   ├── threshold_cache.py           # ThresholdCache (обновление каждые 5 мин из devices)
│   └── alerts.py                    # AlertService (evaluate + MQTT publish + DB write)
└── infra/
    └── db.py                        # Schema creation (readings hypertable, alerts table)
```

## Protobuf-импорты

```python
from placebrain_contracts import collector_pb2 as collector_pb
from placebrain_contracts import devices_pb2 as devices_pb
```

## Таблицы

- `readings` (hypertable: time, device_id, key, value) — TimescaleDB с compression и retention
- `alerts` (sensor_id, threshold_id, device_id, place_id, key, value, threshold_value, threshold_type, severity)

## Буферизация

- In-memory буфер, до 1000 записей или 60 сек
- Flush через `asyncpg COPY`

## Threshold cache

- Обновляется каждые 5 мин из devices-сервиса через gRPC `GetAllThresholds`
- При нарушении порога → запись в alerts + публикация в MQTT `placebrain/{place_id}/alerts`

## gRPC

- `GetLatestReadings(device_id)` — `SELECT DISTINCT ON (key) ... ORDER BY time DESC`
- `GetReadings(device_id, from, to, interval_seconds, keys)` — raw (interval=0, max 2h) или aggregated (time_bucket_gapfill)
- `DeleteReadings(device_ids)` — каскадное удаление alerts + readings

## MQTT

- Username `collector`, пароль `collector` (захардкожен в devices-сервисе как доверенный)
- Подписка: `placebrain/+/devices/+/telemetry`, `placebrain/+/devices/+/status`
- Зависит от: postgres, MQTT-брокер, devices-сервис (gRPC)
