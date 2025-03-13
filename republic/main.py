import random
import sys
import os
from concurrent.futures import ThreadPoolExecutor
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

pandas.io.formats.excel.ExcelFormatter.header_style = None

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru,en;q=0.9",
    "cart-id": "null",
    "city-id": "1",
    "if-none-match": 'W/"8b16b19e2406ea8926c82163545df74e"',
    "jwt-auth-token": "null",
    "mbid": "91dedfdd-2f1c-432f-85e0-a5806795d0f6",
    "origin": "https://www.respublica.ru",
    "priority": "u=1, i",
    "referer": "https://www.respublica.ru/knigi",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
}
BASE_URL = "https://api.respublica.ru/api/v1/listing/"

result = []
count = 1


async def get_item_data(session, book):
    global count
    sku = book.get("attributes").get("sku")
    await asyncio.sleep(random.randint(2, 8))

    # Find stock
    async with session.get(
        f"https://api.respublica.ru/api/v1/items/get/{sku}/delivery_points?query=",
        headers=headers,
    ) as resp:
        resp = await resp.json()
        pick_list = resp.get("points").get("pickup")
        stock = None
        for i in pick_list:
            if "Маяковская" in i["title"]:
                stock = i["available"]
        if not stock:
            print(f"\rDone - {count}", end="")
            count += 1
            return

    async with session.get(
        f"https://api.respublica.ru/api/v1/items/get/{sku}", headers=headers
    ) as resp:
        book_data = await resp.json()
        pure_data = book_data.get("item").get("data").get("attributes")
        title = pure_data.get("title")
        img = pure_data.get("image").get("media").get("url")

    book_data = {"Название": title, "Фото": img, "Наличие": stock}
    result.append(book_data)

    print(f"\rDone - {count}", end="")
    count += 1


async def get_main_data(session, category):
    cat_link = f"{BASE_URL}{category.get("url")}"
    async with session.get(cat_link, headers=headers) as resp:
        cat_data = await resp.json()
        paginator = cat_data.get("pagination").get("pages")

    for i in range(1, int(paginator) + 1):
        async with session.get(f"{cat_link}?page={i}", headers=headers) as resp:
            resp_dict = await resp.json()
            books_on_page = resp_dict.get("items").get("data")

        book_tasks = [
            asyncio.create_task(get_item_data(session, book)) for book in books_on_page
        ]
        await asyncio.gather(*book_tasks)


async def get_gather_data():
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit=10)
    ) as session:
        async with session.get(f"{BASE_URL}/knigi", headers=headers) as response:

            all_categories = await response.json()
            all_categories = all_categories.get("childs")

        tasks = []
        for category in all_categories:
            task = asyncio.create_task(get_main_data(session, category))
            tasks.append(task)
        await asyncio.gather(*tasks)

        df = pd.DataFrame(result).drop_duplicates()
        df.to_excel("test_republica.xlsx", index=False)


def main():
    asyncio.run(get_gather_data())


if __name__ == "__main__":
    main()
