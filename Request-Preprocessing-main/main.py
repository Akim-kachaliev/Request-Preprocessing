import asyncio

from loguru import logger
import uvicorn

from backend.app import create_app
from backend.classification import Classifier
from backend.config import Settings, get_settings
from backend.models import ClassificationResult, TaskMessage
from backend.worker import run_worker, start_worker as _start_worker

from ml.model import Ranker
from ml.ranker_scripts.use_ranker import create_ranker, predict


def start_ml(
    ranker_weights: str,
    device: str = 'cpu'
):
    """
    Создаёт и возвращает экземпляр Ranker на основе весов CatBoost.

    Параметры:
        ranker_weights — путь к файлу обученной модели (.cbm)
        device — устройство для инференса ('cpu' или 'cuda')
    """
    ranker = Ranker(create_ranker(path=ranker_weights), predict)
    return ranker


def create_ranker_function(
        ranker: Ranker,
):
    """
    Оборачивает Ranker в асинхронную функцию-классификатор,
    совместимую с интерфейсом Classifier из backend.classification.

    Возвращает async-функцию, которая принимает TaskMessage,
    вызывает предсказание ранга и возвращает ClassificationResult.
    """
    async def process(task: TaskMessage) -> ClassificationResult:
        logger.info("Обработка задачи {} начата", task.id)
        rank = ranker.predict(task.data)
        logger.info("Обработка задачи {} завершена", task.id)
        return ClassificationResult(rank=rank, details={"source": "ml_process"})

    return process


def start_backend(
    classify_fn: Classifier | None = None,
    *,
    host: str | None = None,
    port: int | None = None,
    start_worker: bool = True,
) -> None:
    """
    Главная точка входа для запуска backend'а.
    Поднимает FastAPI-сервер и, опционально, локальный Celery-воркер.

    Параметры:
        classify_fn — функция классификации (если None — используется fake_process)
        host — хост для привязки (по умолчанию из .env или 0.0.0.0)
        port — порт (по умолчанию из .env или 8000)
        start_worker — запускать ли воркер в том же процессе
    """
    settings = get_settings()
    asyncio.run(
        _serve(
            classify_fn=classify_fn,
            settings=settings,
            host=host or settings.app_host,
            port=port or settings.app_port,
            start_worker=start_worker,
        )
    )


def start_processing_worker(classify_fn: Classifier | None = None) -> None:
    """Запускает только воркер (без API). Удобно для раздельного деплоя."""
    _start_worker(classify_fn=classify_fn, settings=get_settings())


async def _serve(
    classify_fn: Classifier | None,
    settings: Settings,
    host: str,
    port: int,
    start_worker: bool,
) -> None:
    """
    Внутренняя функция: запускает uvicorn-сервер и, если нужно, воркер.
    При остановке сервера корректно завершает воркер.
    """
    worker_task: asyncio.Task[None] | None = None
    if start_worker:
        worker_task = asyncio.create_task(run_worker(classify_fn=classify_fn, settings=settings))

    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings),
            host=host,
            port=port,
            reload=False,
        )
    )

    try:
        await server.serve()
    finally:
        if worker_task is not None:
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)


if __name__ == "__main__":
    # Точка входа при запуске через python main.py
    ranker = start_ml(
        ranker_weights='ml/data/ranker_model.cbm',
        device='cpu'
    )
    start_backend(classify_fn=create_ranker_function(ranker=ranker))
