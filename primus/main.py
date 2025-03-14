import random
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor
import pandas.io.formats.excel
import requests
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger
from openpyxl.styles.builtins import title
from selenium_data import get_book_data

BASE_URL = "https://primusversus.com/collection/all"
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
    # 'Cookie': 'ins_myshop-bsn204=a139bs-f639ac69870ee118ecc5784f648747fc; first_current_location=%2F; first_referer=; referer=; current_location=%2F; visit=t; _ym_uid=1741932225186172611; _ym_d=1741932225; _ym_visorc=w; _ym_isad=2; ins_order_version=1741932316.2066066; cart=%7B%22comment%22%3Anull%2C%22payment_title%22%3Anull%2C%22payment_description%22%3Anull%2C%22delivery_description%22%3Anull%2C%22delivery_price%22%3A0.0%2C%22number%22%3Anull%2C%22delivery_date%22%3Anull%2C%22delivery_from_hour%22%3Anull%2C%22delivery_to_hour%22%3Anull%2C%22delivery_title%22%3Anull%2C%22delivery_from_minutes%22%3Anull%2C%22delivery_to_minutes%22%3Anull%2C%22items_count%22%3A0%2C%22items_price%22%3A0.0%2C%22order_lines%22%3A%5B%5D%2C%22discounts%22%3A%5B%5D%2C%22total_price%22%3A0.0%7D; x_csrf_token=Yus7AsmS7CL3qk2T4fUHURNUkJ-NautTlPhCOGcPRTbBSdaA5DYGbJBgpQQFKYD2OdR_LPXAi9AHhs35gOQtmQ',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

result = []
count = 1


def get_item_data(link):
    global count
    try:
        response = get_book_data(f"https://primusversus.com{link}")
        # async with session.get(
        #     f"https://primusversus.com{link}", headers=headers
        # ) as response:
        soup = bs(response, "lxml")

        title = soup.find("h1").text.strip()
        img = soup.find("picture").find("img").get("data-src")
        stock = soup.find("div", attrs={"class": "add-cart-counter"}).get(
            "data-add-cart-counter-max-quantity"
        )
        book_res = {"Название": title, "Фото": img, "В наличии": stock}
        print(book_res)
        result.append(book_res)
        print(f"\rDone - {count}", end="")
        count += 1
    except Exception as e:
        logger.exception(e)


async def get_gather_data():
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit=1), headers=headers
    ) as session:
        for i in range(1, 152):
            async with session.get(f"{BASE_URL}?page={i}") as resp:
                soup = bs(await resp.text(), "lxml")
                all_products_on_page = soup.find_all(
                    "div", class_="product-preview-elem"
                )
                all_products_on_page = [
                    book.find("a").get("href")
                    for book in all_products_on_page
                    if book.find(
                        "div", class_="product-preview__available"
                    ).text.strip()
                    == "В наличии"
                ]

            for link in all_products_on_page:
                get_item_data(link)
            # with ThreadPoolExecutor(max_workers=1) as executor:
            #     threads = [
            #         executor.submit(get_item_data, link)
            #         for link in all_products_on_page
            #     ]
            #     for i in threads:
            #         try:
            #             i.result()
            #         except Exception as e:
            #             logger.error(e)


def main():
    asyncio.run(get_gather_data())
    df = pd.DataFrame(result)
    df.to_excel("primus.xlsx", index=False)


if __name__ == "__main__":
    start = time.time()
    main()
    print((time.time() - start) / 60)
