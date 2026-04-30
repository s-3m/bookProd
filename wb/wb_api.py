from typing import Any, Literal

import requests
from loguru import logger


class Wildberries:
    def __init__(self, api_token):
        self.headers = {"Authorization": f"{api_token}"}

    def get_items_list(self) -> list[dict[str, Any]]:
        result = []
        body = {
            "settings": {
                "sort": {"ascending": True},
                "filter": {},
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
            for i in raw_list["cards"]:
                result.append(i)
                if raw_list["cursor"]["total"] < 100:
                    break
                body["settings"]["cursor"] = raw_list["cursor"]

        return result

    def update_stocks(self, array_of_items: list[dict[str, str]]):
        body_data = [
            {"chrtId": i["article"], "amount": i["stock"]} for i in array_of_items
        ]
        warehouse_id = ""

        for i in range(0, len(body_data), 1000):
            body = {"stocks": body_data[i : i + 1000]}
            try:
                response = requests.put(
                    f"https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}",
                    headers=self.headers,
                    json=body,
                )
                if response.status_code != 204:
                    logger.warning(f"Ошибка обновления остатков WB - {response.json()}")
            except Exception as e:
                logger.exception(e)
