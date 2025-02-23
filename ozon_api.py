import requests
import os


class Ozon:
    def __init__(self, client_id: str, api_key: str):
        self.client_id = client_id
        self.api_key = api_key
        self.host = "https://api-seller.ozon.ru"

        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def _get_warehouse_id(self):
        response = requests.post(f"{self.host}/v1/warehouse/list", headers=self.headers)
        warehouses_list: list[dict] = response.json().get("result")
        for i in warehouses_list:
            if i["name"] == "Волгоградка":
                return int(i["warehouse_id"])

    def update_stock(self, item_list: list[tuple[str, int]]):
        warehouse_id = self._get_warehouse_id()
        stocks_list = [
            {
                "offer_id": i[0],
                "stock": i[1],
                "warehouse_id": warehouse_id,
            }
            for i in item_list
        ]

        body = {
            "stocks": stocks_list,
        }

        response = requests.post(
            f"{self.host}/v2/products/stocks", headers=self.headers, json=body
        )
        return response.json()


ozon = Ozon(client_id=os.getenv("client_id"), api_key=os.getenv("api_key"))

test = [
    ("9781780742502.0", 22),
    ("9780008374709.0", 33),
    ("9781409181217.0", 44),
    ("9781529053913.0", 13),
    ("9780008437060.0", 11),
    ("4343434555666.0", 100),
]

print(ozon.update_stock(test))
