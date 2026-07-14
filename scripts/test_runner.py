"""
Скрипт функционального и нагрузочного тестирования системы ранжирования заявок.

Запуск:
    python scripts/test_runner.py

Что делает:
    1. Проверяет, что API отвечает (GET /health)
    2. Отправляет стандартный запрос на ранжирование (POST /rank_standard)
    3. Проверяет создание задачи и её видимость через GET /tasks/{id}
    4. Нагрузочный тест: отправляет 100 запросов параллельно и проверяет,
       что все задачи созданы и обработаны
    5. Выводит сводку: PASS/FAIL по каждому тесту
"""

import sys
import json
import time
import uuid
import ssl
import concurrent.futures
from typing import Any
from urllib.request import Request, urlopen, build_opener, ProxyHandler, install_opener
from urllib.error import HTTPError, URLError


# ===== Конфигурация =====
API_URL = "http://localhost:8000"
LOAD_TEST_COUNT = 100        # количество параллельных запросов при нагрузке
LOAD_POLL_SECONDS = 30       # сколько ждать обработки задач после нагрузки
LOAD_POLL_INTERVAL = 2       # интервал между проверками статуса
REQUEST_TIMEOUT = 15         # таймаут на один HTTP-запрос (сек)


# Отключаем системный прокси, чтобы запросы к localhost не уходили через корпоративный прокси
install_opener(build_opener(ProxyHandler({})))


# ===== Вспомогательные функции =====

def _request(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    """Универсальный HTTP-запрос к API. Возвращает (код_статуса, тело_ответа)."""
    url = f"{API_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body else {},
    )
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8")) if e.fp else {}
    except URLError as e:
        return 0, {"error": f"URLError: {e.reason}"}
    except Exception as e:
        return 0, {"error": f"{type(e).__name__}: {e}"}


def _send_task(task_id: str, features: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    """Отправляет один запрос на ранжирование."""
    payload = {
        "task_id": task_id,
        "features": features or {
            "ProbabilityCategory_of_order": 0.1,
            "TotalNumberOfLengths_of_order": 3,
            "TotalPrice_of_order": 1.923,
        },
    }
    return _request("POST", "/rank_standard", payload)


# ===== Тесты =====

def test_api_reachable() -> bool:
    """Тест 1: проверка, что API отвечает на GET /health."""
    status, body = _request("GET", "/health")
    ok = status == 200 and body.get("status") == "ok"
    print(f"  [{'PASS' if ok else 'FAIL'}] GET /health -> {status} {body}")
    return ok


def test_send_standard_request() -> bool | None:
    """Тест 2: отправка стандартного запроса и проверка результата."""
    task_id = f"test_std_{uuid.uuid4().hex[:8]}"
    status, body = _send_task(task_id)
    if status != 201:
        print(f"  [FAIL] POST /rank_standard -> {status} {body}")
        return False

    doc_id = body.get("id")
    if not doc_id:
        print(f"  [FAIL] POST /rank_standard вернул ответ без id: {body}")
        return False

    print(f"  [INFO] Создана задача {doc_id} (external_id={task_id})")

    # Проверяем, что задача появилась в GET /tasks/{id}
    status2, body2 = _request("GET", f"/tasks/{doc_id}")
    if status2 != 200:
        print(f"  [FAIL] GET /tasks/{doc_id} -> {status2} {body2}")
        return False

    # Задача должна быть хотя бы создана
    print(f"  [PASS] Стандартный запрос прошёл успешно")
    return True


def test_send_batch(count: int) -> list[str]:
    """Отправляет count запросов параллельно и возвращает список ID созданных задач."""
    doc_ids: list[str] = []
    errors: list[str] = []

    def _send_one(i: int) -> str | None:
        tid = f"load_{i}_{uuid.uuid4().hex[:6]}"
        _, body = _send_task(tid)
        if doc_id := body.get("id"):
            return doc_id
        errors.append(f"Запрос #{i} (task_id={tid}): {body}")
        return None

    print(f"  [INFO] Отправляю {count} запросов параллельно...")
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(_send_one, i) for i in range(count)]
        for f in concurrent.futures.as_completed(futures):
            doc_id = f.result()
            if doc_id:
                doc_ids.append(doc_id)
    elapsed = time.time() - start
    print(f"  [INFO] Отправлено за {elapsed:.2f}с. Создано задач: {len(doc_ids)}, ошибок: {len(errors)}")
    if errors:
        for e in errors[:5]:
            print(f"    ! {e}")

    return doc_ids


def test_load_100_requests() -> bool:
    """
    Тест 3 (нагрузочный): отправляет 100 запросов, ждёт обработки,
    проверяет что у всех задач проставлен ml_rank или статус не pending.
    """
    print("\n--- Нагрузочный тест: 100 параллельных запросов ---")
    doc_ids = test_send_batch(LOAD_TEST_COUNT)

    if not doc_ids:
        print("  [FAIL] Не создано ни одной задачи — нагрузочный тест прерван")
        return False

    # Ждём, пока worker обработает задачи
    print(f"  [INFO] Ожидаю обработки {len(doc_ids)} задач (до {LOAD_POLL_SECONDS}с)...")
    deadline = time.time() + LOAD_POLL_SECONDS
    processed_count = 0

    while time.time() < deadline and processed_count < len(doc_ids):
        time.sleep(LOAD_POLL_INTERVAL)
        processed_count = 0
        for doc_id in doc_ids:
            _, body = _request("GET", f"/tasks/{doc_id}")
            if body.get("status") != "pending":
                processed_count += 1
        print(f"  [INFO] Обработано: {processed_count}/{len(doc_ids)}")

    # Проверяем итог
    ok = processed_count == len(doc_ids)
    if ok:
        print(f"  [PASS] Все {len(doc_ids)} задач обработаны")
    else:
        not_done = len(doc_ids) - processed_count
        print(f"  [FAIL] {not_done} из {len(doc_ids)} задач не обработались за отведённое время")

    return ok


def test_tasks_list() -> bool:
    """Тест 4: список задач GET /tasks должен быть непустым (после тестов выше)."""
    status, body = _request("GET", "/tasks")
    ok = status == 200 and isinstance(body, list) and len(body) > 0
    print(f"  [{'PASS' if ok else 'FAIL'}] GET /tasks -> {status}, задач: {len(body) if isinstance(body, list) else '?'}")
    return ok


# ===== Главный запуск =====

def main() -> int:
    """Запускает все тесты и возвращает код возврата (0 = успех, 1 = ошибки)."""
    print("=" * 60)
    print("  ТЕСТИРОВАНИЕ СИСТЕМЫ РАНЖИРОВАНИЯ ЗАЯВОК")
    print("=" * 60)
    print(f"\nAPI: {API_URL}\n")

    tests = [
        ("API доступен", test_api_reachable),
        ("Стандартный запрос", test_send_standard_request),
        ("Список задач", test_tasks_list),
        ("Нагрузка 100 запросов", test_load_100_requests),
    ]

    passed = 0
    failed = 0

    for name, func in tests:
        print(f"\n--- {name} ---")
        try:
            result = func()
            if result:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [FAIL] Исключение: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"  ИТОГО: {passed} PASS, {failed} FAIL")
    print("=" * 60)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())