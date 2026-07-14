import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer
from bson import ObjectId
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from .config import Settings, get_settings
from .models import (
    MlResultPayload,
    RankStandardRequest,
    TaskMessage,
    CompleteTaskPayload,
    utc_now_iso,
)

_LOGGER_CONFIGURED = False


def create_app(settings: Settings | None = None) -> FastAPI:
    """
    Создаёт и настраивает FastAPI-приложение с роутами, CORS и подключением
    к MongoDB и Kafka. Все внешние зависимости (клиенты БД, продюсер) хранятся
    в app.state и инициализируются при старте.
    """
    global _LOGGER_CONFIGURED
    settings = settings or get_settings()
    app = FastAPI(title="Request Ranking Backend")

    # Разрешаем CORS-запросы с любых источников (для тестов и дашборда)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Настраиваем логгирование в файл с ротацией при достижении 1 МБ
    if not _LOGGER_CONFIGURED:
        logger.add("app.log", rotation="1 MB")
        _LOGGER_CONFIGURED = True

    @app.on_event("startup")
    async def startup() -> None:
        app.state.settings = settings
        app.state.mongo_client = AsyncIOMotorClient(settings.mongo_uri)
        app.state.db = app.state.mongo_client[settings.mongo_db]

        app.state.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        app.state._producer_started = False
        for attempt in range(1, 4):
            try:
                await asyncio.wait_for(app.state.producer.start(), timeout=10.0)
                app.state._producer_started = True
                logger.info("Kafka producer started (attempt {})", attempt)
                break
            except Exception as exc:
                logger.warning("Kafka producer start failed (attempt {}/3): {}", attempt, exc)
                if attempt < 3:
                    await asyncio.sleep(2)
                else:
                    logger.error("Kafka producer could not be started after 3 attempts")
                    await app.state.producer.stop()
                    app.state.producer = None

        logger.info(
            "Backend started. Mongo DB: {}. Kafka topic: {}",
            settings.mongo_db,
            settings.kafka_request_topic,
        )

    @app.on_event("shutdown")
    async def shutdown() -> None:
        mongo_client: AsyncIOMotorClient = app.state.mongo_client
        producer = getattr(app.state, 'producer', None)
        if producer is not None and getattr(app.state, '_producer_started', False):
            await producer.stop()
            logger.info("Kafka producer stopped")
        mongo_client.close()
        logger.info("Backend stopped")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/rank_standard", status_code=201)
    async def rank_standard(payload: RankStandardRequest, request: Request) -> dict[str, str]:
        return await _create_task(
            request=request,
            external_id=payload.task_id,
            task_type="standard",
            data=payload.features,
        )

    @app.get("/tasks")
    async def get_tasks(request: Request) -> list[dict[str, Any]]:
        docs = await request.app.state.db.tasks.find().to_list(100)
        return [_serialize_doc(doc) for doc in docs]

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str, request: Request) -> dict[str, Any]:
        doc = await _get_task_by_id(request, task_id)
        return _serialize_doc(doc)

    @app.patch("/tasks/{task_id}/ml-result")
    async def set_ml_result(task_id: str, payload: MlResultPayload, request: Request) -> dict[str, str]:
        update_result = await request.app.state.db.tasks.update_one(
            {"_id": _to_object_id(task_id)},
            {
                "$set": {
                    "ml_rank": payload.rank,
                    "status": payload.status,
                    "ml_details": payload.details,
                    "updated_at": utc_now_iso(),
                }
            },
        )
        if update_result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"id": task_id, "status": payload.status}

    @app.patch("/tasks/{task_id}/complete")
    async def complete_task(task_id: str, payload: CompleteTaskPayload, request: Request) -> dict[str, str]:
        update_result = await request.app.state.db.tasks.update_one(
            {"_id": _to_object_id(task_id)},
            {
                "$set": {
                    "status": "Готово",
                    "completed_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                    "comment": payload.comment,
                }
            },
        )
        if update_result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"id": task_id, "status": "Готово"}

    return app


async def _create_task(
    request: Request,
    external_id: str,
    task_type: str,
    data: dict[str, Any],
) -> dict[str, str]:
    doc = {
        "external_id": external_id,
        "type": task_type,
        "data": data,
        "status": "pending",
        "created_at": utc_now_iso(),
    }
    result = await request.app.state.db.tasks.insert_one(doc)
    doc_id = str(result.inserted_id)

    message = TaskMessage(
        id=doc_id,
        external_id=external_id,
        type=task_type,
        data=data,
        status="pending",
        created_at=doc["created_at"],
    )

    settings: Settings = request.app.state.settings
    producer = getattr(request.app.state, 'producer', None)
    if producer is not None and getattr(request.app.state, '_producer_started', False):
        await producer.send_and_wait(
            settings.kafka_request_topic,
            json.dumps(message.model_dump(), ensure_ascii=False).encode("utf-8"),
        )
        logger.info("Task {} created and published to Kafka", doc_id)
        return {"id": doc_id, "status": "queued"}
    else:
        logger.warning("Task {} created but Kafka unavailable — result will not be processed", doc_id)
        return {"id": doc_id, "status": "queued (kafka unavailable)"}


async def _get_task_by_id(request: Request, task_id: str) -> dict[str, Any]:
    doc = await request.app.state.db.tasks.find_one({"_id": _to_object_id(task_id)})
    if doc is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return doc


def _to_object_id(task_id: str) -> ObjectId:
    try:
        return ObjectId(task_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid task id") from exc


def _serialize_doc(doc: dict[str, Any]) -> dict[str, Any]:
    return {**doc, "_id": str(doc["_id"])}