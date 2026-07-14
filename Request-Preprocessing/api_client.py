import requests
import time
import uuid


class MoskabelmetAPI:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url

    def send_standard(self, features: dict) -> str:
        """Отправка JSON с фичами (как в test_request.json)"""
        payload = {
            "task_id": f"std_{uuid.uuid4().hex[:6]}",
            "features": features
        }
        res = requests.post(f"{self.base_url}/rank_standart", json=payload)
        res.raise_for_status()
        return res.json()["id"]

    def send_letter(self, text: str) -> str:
        """Отправка текста письма"""
        payload = {
            "task_id": f"let_{uuid.uuid4().hex[:6]}",
            "content": text
        }
        res = requests.post(f"{self.base_url}/rank_letter", json=payload)
        res.raise_for_status()
        return res.json()["id"]

    def get_result(self, task_id: str, timeout: int = 60) -> dict:
        """
        Ожидание результата из MongoDB.
        Возвращает: ml_rank, ml_probability, ml_md, status
        """
        start = time.time()
        while time.time() - start < timeout:
            response = requests.get(f"{self.base_url}/tasks/{task_id}")

            # 404 — задача ещё не появилась в БД, ждём
            if response.status_code == 404:
                print(f"Задача {task_id} ещё не создана, ждём...")
                time.sleep(2)
                continue

            response.raise_for_status()
            data = response.json()

            if data.get("ml_rank") is not None or data.get("status") == "Выполнено":
                return {
                    "ml_rank":        data.get("ml_rank"),
                    "ml_probability": data.get("ml_details", {}).get("probability"),
                    "ml_md":          data.get("ml_details", {}).get("md"),
                    "status":         data.get("status"),
                }

            print(f"Задача {task_id} в обработке... (Статус: {data.get('status')})")
            time.sleep(2)

        return {"error": "Превышено время ожидания"}


if __name__ == "__main__":
    api = MoskabelmetAPI()

    print("--- Тест письма ---")
    l_id = api.send_letter("Прошу отгрузить Кабель ВВГнг-LS 3х2.5 в количестве 100 метров в Москву")
    result = api.get_result(l_id)
    print("Ранг:       ", result.get("ml_rank"))
    print("Вероятность:", result.get("ml_probability"))
    print("МД:         ", result.get("ml_md"))
    print("\n--- Тест признаков ---")
    sample_features = {"ProbabilityCategory_of_order": 0.1, "TotalNumberOfLengths_of_order": 3}
    s_id = api.send_standard(sample_features)
    result_std = api.get_result(s_id)
    print("Ранг:       ", result_std.get("ml_rank"))
    print("Вероятность:", result_std.get("ml_probability"))
    print("МД:         ", result_std.get("ml_md"))