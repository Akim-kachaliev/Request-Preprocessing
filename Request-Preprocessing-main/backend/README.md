# Request Ranking Backend

Backend-часть сервиса для приёма заявок на ранжирование, отправки их в Kafka и сохранения результатов ML-обработки в MongoDB.

---

## Архитектура backend'а

```
HTTP-запрос → FastAPI → MongoDB (сохранить задачу)
                      → Kafka (опубликовать сообщение)
                            → Worker (Kafka consumer)
                                → ML-функция (ранжирование)
                                → MongoDB (записать ml_rank)
```

Backend состоит из двух логических частей, которые могут работать как в одном процессе, так и раздельно:

1. **API-сервер** (FastAPI) — принимает HTTP-запросы, сохраняет задачи в MongoDB, публикует в Kafka
2. **Worker** (asyncio + aiokafka) — читает задачи из Kafka, вызывает ML-функцию, записывает результат

---

## API

### `GET /health`

Проверка работоспособности сервера.

**Ответ:**
```json
{"status": "ok"}
```

---

### `POST /rank_standard`

Создаёт задачу на ранжирование по признакам заявки.

**Тело запроса:**
```json
{
  "task_id": "std_demo_001",
  "features": {
    "ProbabilityCategory_of_order": 0.1,
    "TotalNumberOfLengths_of_order": 3,
    "TotalPrice_of_order": 1.923
  }
}
```

**Параметры:**
- `task_id` — уникальный внешний идентификатор задачи (строка, обязателен)
- `features` — словарь признаков заявки для ML-модели

**Ответ (201):**
```json
{
  "id": "66a1b2c3d4e5f67890123456",
  "status": "queued"
}
```

Если Kafka недоступен, статус будет `"queued (kafka unavailable)"` — задача сохранена, но не будет обработана.

---

### `GET /tasks`

Возвращает список последних 100 задач из MongoDB.

**Ответ:**
```json
[
  {
    "_id": "66a1b2c3d4e5f67890123456",
    "external_id": "std_demo_001",
    "type": "standard",
    "data": { ... },
    "status": "Ожидает",
    "ml_rank": 0.85,
    "created_at": "2026-07-07T12:00:00+00:00"
  }
]
```

---

### `GET /tasks/{task_id}`

Возвращает одну задачу по её MongoDB `_id`.

**Параметры:**
- `task_id` — MongoDB ObjectId (24 hex-символа)

**Ответ:** объект задачи (см. выше).

---

### `PATCH /tasks/{task_id}/ml-result`

Позволяет вручную записать результат ML-обработки в базу. Полезно для отладки или когда ML-модуль работает как внешний сервис.

**Тело запроса:**
```json
{
  "rank": 0.87,
  "status": "Ожидает",
  "details": {
    "source": "manual_script",
    "model_version": "1.0"
  }
}
```

**Параметры:**
- `rank` — числовой ранг приоритета (float, int или строка)
- `status` — статус задачи (по умолчанию `"Ожидает"`)
- `details` — произвольные служебные данные (опционально)

---

### `PATCH /tasks/{task_id}/complete`

Переводит задачу в статус `"Готово"` — после того, как оператор отработал заявку.

**Тело запроса:**
```json
{
  "comment": "Заявка обработана, отгружено"
}
```

**Параметры:**
- `comment` — комментарий оператора (опционально)

---

## Переменные окружения

Все настройки читаются из `.env` файла или переменных окружения системы.

```env
MONGO_URI=mongodb://localhost:27017
MONGO_DB=ranking_db
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=requests.created
KAFKA_CONSUMER_GROUP=request-ranking-backend
APP_HOST=0.0.0.0
APP_PORT=8000
```

Поддерживаются оба имени для топика: `KAFKA_TOPIC` и `KAFKA_REQUEST_TOPIC`.

---

## Скрипты

В папке `scripts/` находятся утилиты для повседневной работы:

### `send_standard_request.bat`

Отправляет тестовый запрос на ранжирование с признаками из файла `payloads/standard_request.json`.

```powershell
scripts\send_standard_request.bat
```

### `run_tests.bat`

Запускает функциональные и нагрузочные тесты.

```powershell
scripts\run_tests.bat
```

### `test_runner.py`

Python-скрипт с тестами (вызывается из `run_tests.bat`). Содержит:
- Проверку `GET /health`
- Отправку стандартного запроса
- Проверку списка задач
- Нагрузочный тест (100 параллельных запросов)

---

## Как подключить свою ML-функцию

### Вариант 1: Передать функцию в `start_backend`

```python
from main import start_backend
from backend.models import TaskMessage

async def my_classifier(task: TaskMessage):
    # Ваша логика ранжирования
    return {
        "rank": 0.91,
        "status": "Ожидает",
        "details": {"model": "v2"},
    }

start_backend(classify_fn=my_classifier)
```

Функция может быть как синхронной (`def`), так и асинхронной (`async def`). Результат может быть:
- `ClassificationResult` — предпочтительно
- `dict` — должен содержать ключ `rank`
- `float` / `int` / `str` — будет использован как ранг

### Вариант 2: Использовать `fake_process` (по умолчанию)

Если `classify_fn=None`, используется `fake_process` из `backend/classification.py` — заглушка, которая ждёт 5 секунд и возвращает ранг `0.5`. Подходит для тестирования без ML-модели.

### Вариант 3: Запустить worker отдельно

```python
from main import start_processing_worker
start_processing_worker()
```

Worker запустится как отдельный процесс, подписанный на Kafka. API-сервер при этом не нужен.

---

## Модели данных

Все модели находятся в `backend/models.py`:

- `RankStandardRequest` — входящий запрос на ранжирование
- `TaskMessage` — сообщение для Kafka
- `ClassificationResult` — результат классификации
- `MlResultPayload` — payload для ручной записи ML-результата
- `CompleteTaskPayload` — payload для завершения задачи

---

## Логирование

Логи пишутся через `loguru`:
- В консоль (stdout)
- В файл `app.log` с ротацией при достижении 1 МБ

Формат: время, уровень, сообщение.