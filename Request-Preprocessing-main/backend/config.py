import os
from dataclasses import dataclass

from dotenv import load_dotenv


# Загружаем переменные из .env файла (если есть)
load_dotenv()


@dataclass(frozen=True)
class Settings:
    """
    Неизменяемая конфигурация приложения.
    Все поля читаются из переменных окружения (или .env файла).

    Поля:
        mongo_uri — строка подключения к MongoDB
        mongo_db — имя базы данных
        kafka_bootstrap_servers — адрес Kafka-кластера
        kafka_request_topic — топик для публикации задач
        kafka_consumer_group — группа потребителей Kafka
        app_host — хост для привязки FastAPI
        app_port — порт для FastAPI
    """
    mongo_uri: str
    mongo_db: str
    kafka_bootstrap_servers: str
    kafka_request_topic: str
    kafka_consumer_group: str
    app_host: str
    app_port: int


def get_settings() -> Settings:
    """
    Собирает и возвращает объект Settings из переменных окружения.
    Если переменная не задана — используется значение по умолчанию.
    """
    # Поддерживаем оба имени переменной: KAFKA_REQUEST_TOPIC и KAFKA_TOPIC
    request_topic = os.getenv("KAFKA_REQUEST_TOPIC") or os.getenv("KAFKA_TOPIC") or "requests.created"

    return Settings(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "ranking_db"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_request_topic=request_topic,
        kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "request-ranking-backend"),
        app_host=os.getenv("APP_HOST", "0.0.0.0"),
        app_port=int(os.getenv("APP_PORT", "8000")),
    )
