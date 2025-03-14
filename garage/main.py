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

BASE_URL = "https://shop.garagemca.org"

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en;q=0.9",
    "cache-control": "max-age=0",
    # 'cookie': 'PHPSESSID=8p9f731k42j7cfdidaa96vedur; _csrf=1e4d9ed26808a8dba0cf9fa1f25e914fc39ae700a71684238be6900caa329ac5a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22ms76EH6-4ca_nSBt5fS8X6H6JSkKkg4F%22%3B%7D; tmr_lvid=6a4fba3af5120806735535caf8c5b142; tmr_lvidTS=1741952528549; webpSupported=01; _ym_uid=1741952529912643436; _ym_d=1741952529; domain_sid=eCLOnpm2d-v3pw08s3y-P%3A1741952528856; _ym_isad=2; _ym_visorc=w; garage_shop_notification=8241f99ef25ca5bad469d87a5aa715c7; tmr_detect=0%7C1741952859977',
    "priority": "u=0, i",
    "referer": "https://shop.garagemca.org/ru/books/?per_page=50&page=3",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
}

result = []
count = 1


async def get_item_data(session, link):
    global count
    book_res = {}
    try:
        async with session.get(f"{BASE_URL}{link}", headers=headers) as resp:
            soup = bs(await resp.text(), "lxml")
            title = soup.find("h1").text.strip()
            img = (
                soup.find("div", attrs={"class": "block gallery"})
                .find("img")
                .get("src")
            )
            book_res["Название"] = title
            book_res["Фото"] = img

            stock_area = soup.find("div", {"class": "stock-cities__column"}).select(
                "div.stock-cities__city:not(.out)"
            )

            for i in stock_area:
                if i.text.startswith("Москва"):
                    book_res[i] = "да"

            result.append(book_res)
            print(f"\rDone - {count}", end="")
            count += 1

    except Exception as e:
        logger.exception(f"{BASE_URL}{link}")


async def get_gather_data():
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit=2), headers=headers
    ) as session:
        async with session.get(
            f"https://shop.garagemca.org/ru/books/?per_page=all"
        ) as resp:
            # time.sleep(50)
            soup = bs(await resp.text(), "lxml")
            all_elem = soup.find("div", {"class": "product-grid"}).find_all(
                "div", {"class": "item"}
            )
            all_links = [
                book.find("a")["href"]
                for book in all_elem
                if not book.find("div", {"class": "caption"})
                .text.strip()
                .startswith("ПРЕДЗАКАЗ")
            ]

            tasks = [
                asyncio.create_task(get_item_data(session, link)) for link in all_links
            ]
            await asyncio.gather(*tasks)


def main():
    asyncio.run(get_gather_data())
    df = pd.DataFrame(result)
    df.to_excel("garage.xlsx", index=False)


if __name__ == "__main__":
    start = time.time()
    main()
    print((time.time() - start) / 60)
