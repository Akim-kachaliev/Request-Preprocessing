import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaConsumer
from bson import ObjectId
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from .classification import Classifier, fake_process, run_classifier
from .config import Settings, get_settings
from .models import ClassificationResult, TaskMessage, utc_now_iso


async def run_worker(
    classify_fn: Classifier | None = None,
    settings: Settings | None = None,
) -> None:
    """
    Основной цикл воркера: читает сообщения из Kafka, вызывает функцию
    классификации и сохраняет результат в MongoDB.

    Параметры:
        classify_fn — функция ранжирования (по умолчанию fake_process)
        settings — конфигурация подключения (Mongo, Kafka)
    """
    settings = settings or get_settings()
    classify_fn = classify_fn or fake_process
    mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    db = mongo_client[settings.mongo_db]
    consumer = AIOKafkaConsumer(
        settings.kafka_request_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        enable_auto_commit=False,       # ручной коммит — после успешной обработки
        auto_offset_reset="earliest",   # читаем с самого раннего непрочитанного
    )

    # Пытаемся подключиться к Kafka до 3 раз с паузой 2с
    consumer_started = False
    for attempt in range(1, 4):
        try:
            await asyncio.wait_for(consumer.start(), timeout=10.0)
            consumer_started = True
            logger.info("Worker subscribed to topic {} (attempt {})", settings.kafka_request_topic, attempt)
            break
        except Exception as exc:
            logger.warning("Kafka consumer start failed (attempt {}/3): {}", attempt, exc)
            if attempt < 3:
                await asyncio.sleep(2)
            else:
                logger.error("Kafka consumer could not be started after 3 attempts")
                await consumer.stop()
                mongo_client.close()
                return

    try:
        # Бесконечный цикл чтения сообщений из Kafka
        async for message in consumer:
            try:
                # Парсим сообщение в TaskMessage
                task = TaskMessage.model_validate(message.value)
                # Ставим статус "processing" в MongoDB
                await db.tasks.update_one(
                    {"_id": ObjectId(task.id)},
                    {"$set": {"status": "processing", "processing_started_at": utc_now_iso()}},
                )

                # Запускаем классификацию
                result = await run_classifier(classify_fn, task)
                # Сохраняем результат (ml_rank, статус, детали)
                await _save_result(db, task.id, result)
                # Коммитим — Kafka запомнит, что сообщение обработано
                await consumer.commit()
                logger.success("Task {} processed. Rank: {}", task.id, result.rank)
            except Exception:
                logger.exception("Failed to process message from Kafka")
    except asyncio.CancelledError:
        logger.info("Worker cancellation received")
        raise
    finally:
        # Корректное закрытие соединений при остановке
        if consumer_started:
            await consumer.stop()
        mongo_client.close()
        logger.info("Worker stopped")


async def _save_result(db: Any, task_id: str, result: ClassificationResult) -> None:
    """Записывает результат классификации в MongoDB (ml_rank, статус, детали)."""
    await db.tasks.update_one(
        {"_id": ObjectId(task_id)},
        {
            "$set": {
                "ml_rank": result.rank,
                "status": result.status,
                "ml_details": result.details,
                "updated_at": utc_now_iso(),
            }
        },
    )


def start_worker(classify_fn: Classifier | None = None, settings: Settings | None = None) -> None:
    """Синхронная обёртка для запуска воркера (используется в main.py)."""
    asyncio.run(run_worker(classify_fn=classify_fn, settings=settings))
