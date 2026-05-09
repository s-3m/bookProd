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
            response = requests.post(
                "https://content-api.wildberries.ru/content/v2/get/cards/list",
                headers=self.headers,
                json=body,
            )
            raw_list = response.json()
            time.sleep(0.4)
            for i in raw_list["cards"]:
                result.append(i)
            if raw_list["cursor"]["total"] < 100:
                break
            body["settings"]["cursor"]["updatedAt"] = raw_list["cursor"]["updatedAt"]
            body["settings"]["cursor"]["nmID"] = raw_list["cursor"]["nmID"]

        return result

    def update_stocks(self, array_of_items: list[dict[str, str]] = None):
        body_data = [
            {"chrtId": int(i["chrtID"]), "amount": int(i["stock"] or 0)}
            for i in array_of_items
        ]

        warehouse_id = 1658946
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
