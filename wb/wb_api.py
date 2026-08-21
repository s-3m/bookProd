import time
from typing import Any, Literal

import requests
from loguru import logger


class Wildberries:
    def __init__(self, api_token):
        self.headers = {
            "Authorization": f"{api_token}",
            "Content-Type": "application/json",
        }

    def _request_with_retry(self, body, max_retries=5):
        last_exception = None

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    "https://content-api.wildberries.ru/content/v2/get/cards/list",
                    headers=self.headers,
                    json=body,
                    timeout=(10, 30),  # (connect_timeout, read_timeout)
                )

                try:
                    data = response.json()
                except ValueError:
                    data = {"error": True, "raw_text": response.text[:500]}

                if response.status_code == 200 and not data.get("error"):
                    return data

                wait = min(2**attempt, 30)
                print(
                    f"WB API error (попытка {attempt + 1}/{max_retries}): "
                    f"status={response.status_code}, body={data}. Ретрай через {wait}с"
                )

            except requests.exceptions.RequestException as e:
                last_exception = e
                wait = min(2**attempt, 30)
                print(
                    f"WB API connection error (попытка {attempt + 1}/{max_retries}): "
                    f"{type(e).__name__}: {e}. Ретрай через {wait}с"
                )

            time.sleep(wait)

        if last_exception:
            raise RuntimeError(
                f"Не удалось получить список карточек после {max_retries} попыток"
            ) from last_exception

        raise RuntimeError(
            f"Не удалось получить список карточек после {max_retries} попыток"
        )

    def get_items_list(self) -> list[dict[str, Any]]:
        result = []
        body = {
            "settings": {
                "sort": {"ascending": True},
                "filter": {
                    "withPhoto": -1,
                    "allowedCategoriesOnly": False,
                },
                "cursor": {"limit": 100},
            }
        }

        while True:
            raw_list = self._request_with_retry(body)

            result.extend(raw_list["cards"])

            if raw_list["cursor"]["total"] < 100:
                break

            body["settings"]["cursor"]["updatedAt"] = raw_list["cursor"]["updatedAt"]
            body["settings"]["cursor"]["nmID"] = raw_list["cursor"]["nmID"]
            time.sleep(0.8)

        return result


    def update_stocks(self, array_of_items: list[dict[str, str]] = None):
        body_data = [
            {"chrtId": int(i["chrtID"]), "amount": int(i["stock"] or 0)}
            for i in array_of_items
        ]

        warehouse_id = self.get_warehouses()[0]["id"]
        session = requests.Session()

        for i in range(0, len(body_data), 1000):
            body = {"stocks": body_data[i : i + 1000]}
            for request in range(5):
                try:
                    time.sleep(1)
                    response = session.put(
                        f"https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}",
                        headers=self.headers,
                        json=body,
                    )
                    if response.status_code == 204:
                        break
                    if response.status_code == 429:
                        time.sleep(5)
                        continue

                    logger.warning(f"Ошибка обновления остатков WB - {response.json()}")
                    break

                except Exception as e:
                    logger.exception(e)
                    time.sleep(5)

    def get_warehouses(self):
        response = requests.get(
            "https://marketplace-api.wildberries.ru/api/v3/warehouses",
            headers=self.headers,
        )
        result = response.json()
        return result

    def get_items_stocks(self):
        response = requests.post(
            "https://marketplace-api.wildberries.ru/api/v3/stocks/1658946",
            headers=self.headers,
            json={"chrtIds": []},
        )
        result = response.json()
        return result
