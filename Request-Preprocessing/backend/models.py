from datetime import datetime, timezone
from typing import Any, Dict, Literal

from pydantic import BaseModel, Field


class RankStandardRequest(BaseModel):
    """Модель запроса на ранжирование (POST /rank_standard).

    task_id — внешний идентификатор задачи (обязателен, непустой)
    features — словарь признаков заявки для ML-модели
    """
    task_id: str = Field(..., min_length=1)
    features: Dict[str, Any]


class TaskMessage(BaseModel):
    """Сообщение, которое публикуется в Kafka для обработки воркером.

    id — MongoDB _id задачи
    external_id — внешний идентификатор, переданный клиентом
    type — тип задачи (пока только 'standard')
    data — признаки заявки
    status — текущий статус ('pending')
    created_at — ISO-временная метка создания
    """
    id: str
    external_id: str
    type: Literal["standard"] = "standard"
    data: Dict[str, Any]
    status: str
    created_at: str


class ClassificationResult(BaseModel):
    """Результат работы классификатора (ML-модели или заглушки).

    rank — числовой ранг приоритета (float, int или строка)
    status — статус задачи после обработки ML (по умолчанию 'Ожидает')
    details — произвольные служебные данные (источник, версия модели и т.п.)
    """
    rank: float | int | str
    status: str = "Ожидает"
    details: Dict[str, Any] = Field(default_factory=dict)


class MlResultPayload(BaseModel):
    """Payload для ручной записи ML-результата (PATCH /tasks/{id}/ml-result).

    Используется для отладки, когда нужно напрямую записать ранг в БД.
    """
    rank: float | int | str
    status: str = "Ожидает"
    details: Dict[str, Any] = Field(default_factory=dict)


class CompleteTaskPayload(BaseModel):
    """Payload для завершения задачи (PATCH /tasks/{id}/complete).

    comment — опциональный комментарий от сотрудника, обработавшего заявку.
    """
    comment: str | None = None


def utc_now_iso() -> str:
    """Возвращает текущее UTC-время в ISO-формате (строка)."""
    return datetime.now(timezone.utc).isoformat()
