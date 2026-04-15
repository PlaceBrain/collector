from pydantic import PostgresDsn
from pydantic_settings import BaseSettings


class LoggingConfig(BaseSettings):
    level: str = "info"
    format: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"


class MqttConfig(BaseSettings):
    url: str = "mqtt://placebrain-emqx:1883"
    username: str = "collector"
    password: str = "collector"


class DatabaseConfig(BaseSettings):
    url: PostgresDsn = "postgresql://backend:dfghy87t6fgvby6@placebrain-database:5432/telemetry_db"  # type: ignore[assignment]
    pool_size: int = 10


class BufferConfig(BaseSettings):
    max_size: int = 1000
    flush_interval: int = 60


class AppConfig(BaseSettings):
    port: int = 50054


class KafkaConfig(BaseSettings):
    url: str = "placebrain-kafka:19092"


class RedisConfig(BaseSettings):
    url: str = "redis://placebrain-redis:6379/0"


class Settings(BaseSettings):
    model_config = {"env_nested_delimiter": "__", "env_file": ".env"}

    app: AppConfig = AppConfig()
    logging: LoggingConfig = LoggingConfig()
    mqtt: MqttConfig = MqttConfig()
    database: DatabaseConfig = DatabaseConfig()
    buffer: BufferConfig = BufferConfig()
    kafka: KafkaConfig = KafkaConfig()
    redis: RedisConfig = RedisConfig()
