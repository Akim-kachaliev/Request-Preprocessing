import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from .models import ClassificationResult, TaskMessage

# Тип, который может вернуть функция классификации: готовый объект,
# число, строка, словарь — всё нормализуется в ClassificationResult.
ClassifierResult = ClassificationResult | float | int | str | dict[str, Any]
# Сигнатура функции классификации: принимает TaskMessage, возвращает ClassifierResult
Classifier = Callable[[TaskMessage], ClassifierResult | Awaitable[ClassifierResult]]


async def fake_process(task: TaskMessage) -> ClassificationResult:
    """
    Заглушка для тестирования: ждёт 5 секунд и возвращает фиктивный ранг 0.5.
    Используется, когда ML-модуль ещё не подключён.
    """
    logger.info("Fake processing started for task {}", task.id)
    await asyncio.sleep(5)
    logger.info("Fake processing finished for task {}", task.id)
    return ClassificationResult(rank=0.5, details={"source": "fake_process"})


async def run_classifier(classify_fn: Classifier, task: TaskMessage) -> ClassificationResult:
    """
    Запускает функцию классификации и нормализует результат.
    Поддерживает как синхронные, так и асинхронные функции.
    """
    result = classify_fn(task)
    if inspect.isawaitable(result):
        result = await result
    return normalize_result(result)


def normalize_result(result: ClassifierResult) -> ClassificationResult:
    """
    Приводит результат классификации к единому типу ClassificationResult.
    Поддерживает:
    - уже готовый ClassificationResult
    - словарь (должен содержать ключ 'rank')
    - число, float, строку (rank = это значение)
    """
    if isinstance(result, ClassificationResult):
        return result

    if isinstance(result, dict):
        if "rank" not in result:
            raise ValueError("Classifier dict result must contain 'rank'")
        return ClassificationResult(**result)

    if isinstance(result, (int, float, str)):
        return ClassificationResult(rank=result)

    raise TypeError(f"Unsupported classifier result type: {type(result)!r}")
