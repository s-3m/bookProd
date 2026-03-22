from typing import Any, Literal
import httpx
from requests import session
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
            base_url=f"https://api.partner.market.yandex.ru/v2/campaigns",
            headers=self.headers,
            timeout=30,
            transport=httpx.HTTPTransport(retries=3),
        )
        return session

    def _make_requests(
        self,
        data: dict[str, Any],
        method: Literal["GET", "POST", "PUT"],
        endpoint: str = "/",
    ) -> dict[str, Any]:

        try:
            response = self.session.request(url=endpoint, json=data, method=method)
            response.raise_for_status()
            if not response.content:
                return {"status": "OK", "message": "Request completed successfully"}

            return response.json()

        except Exception as e:
            logger.exception(e)

    def get_shop_info(self):
        response = self._make_requests(data={}, method="GET", endpoint=self.campaign_id)
        return response


if __name__ == "__main__":
    ya = Market(
        api_key="",
        campaign_id="",
    )
    print(ya.get_shop_info())
