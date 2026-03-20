# Collector Service

- **Порт:** 50054
- **БД:** telemetry_db (TimescaleDB)
- Гибридная архитектура: MQTT-подписчик + gRPC-сервер в одном asyncio event loop
- **Не использует SQLAlchemy/UoW/Repository** — raw asyncpg pool

## Таблицы

- `readings` (hypertable: time, device_id, key, value)
- `alerts`

## Буферизация

- In-memory буфер, до 1000 записей или 60 сек
- Flush через `asyncpg COPY`

## Threshold cache

- Обновляется каждые 5 мин из devices-сервиса
- При нарушении порога → запись в alerts + публикация в MQTT `placebrain/{place_id}/alerts`

## gRPC

- `GetLatestReadings(device_id)` — `SELECT DISTINCT ON (key) ... ORDER BY time DESC`

## MQTT

- Username `collector`, пароль `collector` (захардкожен в devices-сервисе как доверенный)
- Зависит от: postgres, MQTT-брокер, devices-сервис (gRPC)
