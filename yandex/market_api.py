import os
from typing import Any, Literal
import httpx
from loguru import logger


class Market:
    def __init__(self, campaign_id, api_key):
        self.api_key = api_key
        self.campaign_id = campaign_id
        self.headers = {
            "Api-Key": api_key,
        }
        self.session: httpx.Client = self.__make_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

    def __make_session(self):
        session = httpx.Client(
            base_url=f"https://api.partner.market.yandex.ru/v2",
            headers=self.headers,
            timeout=30,
            transport=httpx.HTTPTransport(retries=3),
        )
        return session

    def _make_requests(
        self,
        data: dict[str, Any] | list[dict[str, Any]],
        method: Literal["GET", "POST", "PUT"],
        query_params: dict[str, Any] | None = None,
        endpoint: str = "/",
    ) -> dict[str, Any] | None:

        try:
            response = self.session.request(
                url=endpoint,
                json=data,
                method=method,
                params=query_params if query_params else None,
            )
            response.raise_for_status()
            if not response.content:
                return {"status": "OK", "message": "Request completed successfully"}

            clean_response = response.json()
            if clean_response.get("errors"):
                logger.error(clean_response["errors"])
            return clean_response

        except Exception as e:
            logger.exception(e)

    def get_shop_info(self):
        response = self._make_requests(
            data={}, method="GET", endpoint=f"/campaigns/{self.campaign_id}"
        )
        return response

    def stock_update(self, data: list[dict[str, Any]]):
        errors = False
        list_for_stock_update = [
            {"sku": i["article"], "items": [{"count": i["stock"]}]} for i in data
        ]
        for i in range(0, len(list_for_stock_update), 2000):
            body_data = {"skus": list_for_stock_update[i : i + 2000]}
            response = self._make_requests(
                data=body_data,
                method="PUT",
                endpoint=f"/campaigns/{self.campaign_id}/offers/stocks",
            )
            if response["status"] != "OK":
                errors = True

        if not errors:
            logger.success("Остатки обновлены без ошибок")
        else:
            logger.warning("Есть ошибки при обновлении остатков")

    def price_update(self, data: list[dict[str, Any]]):
        """
        :param data: [{"article": 12345.0, "price": 2500, "discount_price": 5000}]
        :return: None
        """
        errors = False
        list_for_price_update = [
            {
                "offerId": i["article"],
                "price": {
                    "value": i["price"],
                    "discountBase": i["discount_price"],
                    "currencyId": "RUR",
                    "vat": 0,
                },
            }
            for i in data
        ]
        for i in range(0, len(list_for_price_update), 2000):
            body_data = {"offers": list_for_price_update[i : i + 2000]}
            response = self._make_requests(
                data=body_data,
                method="POST",
                endpoint=f"businesses/{self.campaign_id}/offer-prices/updates",
            )
            if response["status"] != "OK":
                errors = True

        if not errors:
            logger.success("Цены обновлены без ошибок")
        else:
            logger.warning("Есть ошибки при обновлении цен")

    def get_items(
        self,
        limit: int = 200,
        category=None,
        article=None,
        statuses=None,
        tags=None,
        vendor_name=None,
    ) -> list[dict]:
        """
        :param limit: limit per response (min 1, max 200)
        :param category: category filter
        :param article: article filter
        :param statuses:
            PUBLISHED — Готов к продаже.
            CHECKING — На проверке.
            DISABLED_BY_PARTNER — Скрыт вами.
            REJECTED_BY_MARKET — Отклонен.
            DISABLED_AUTOMATICALLY — Исправьте ошибки.
            CREATING_CARD — Создается карточка.
            NO_CARD — Нужна карточка.
            NO_STOCKS — Нет на складе.
            ARCHIVED — В архиве.
        :param tags: tags filter
        :param vendor_name: brands name filter
        :return: list of dicts
                            [{'basicPrice': {'currencyId': 'RUR',
                                             'discountBase': 15780.0,
                                             'updatedAt': '2026-03-24T01:34:59+03:00',
                                             'value': 7905.0},
                              'offerId': '1000057.0',
                              'status': 'PUBLISHED'}, ...]
        """
        if statuses is None:
            statuses = ["PUBLISHED", "NO_STOCKS"]

        data_list = []
        request_body = {"statuses": statuses}
        query_params = {"limit": limit, "pageToken": ""}
        while True:
            try:
                response = self._make_requests(
                    data=request_body,
                    query_params=query_params,
                    method="POST",
                    endpoint=f"campaigns/{self.campaign_id}/offers",
                )
            except Exception as e:
                logger.exception(e)
                continue
            data_list.extend(response["result"]["offers"])
            if response["result"]["paging"].get("nextPageToken"):
                query_params["pageToken"] = response["result"]["paging"][
                    "nextPageToken"
                ]
            else:
                break
        print(len(data_list))
        print(data_list[:10])
        return data_list


# if __name__ == "__main__":
#     ya = Market(
#         api_key=os.getenv("YANDEX_API_KEY_MSK"),
#         campaign_id=os.getenv("YANDEX_CAMPAIGN_ID"),
#     )
#     test_data = [
#         {"article": "1213135.0", "stock": 3},
#         {"article": "1117335.0", "stock": 2},
#         {"article": "1183123.0", "stock": 1},
#     ]
    # ya.price_update([{"article": 1213135.0, "price": 15100, "discount_price": 30200}])
    # ya.stock_update(data=test_data)
    # ya.get_items()
