import random
import sys
import os
from concurrent.futures import ThreadPoolExecutor
import pandas.io.formats.excel
import requests
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger
from openpyxl.styles.builtins import title

BASE_URL = "http://primuzee.ru/shop/Knigi"
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
    # 'Cookie': 'PHPSESSID=pn1jqnpguk0m68tt4vh8emd7b2',
    "If-Modified-Since": "Thu, 13 Mar 2025 12:27:27 GMT",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
}

result = []
count = 1


def get_item_data(link):
    global count
    # async with session.get(link, headers=headers) as resp:
    try:
        resp = requests.get(link, headers=headers)
        soup = bs(resp.text, "lxml")
        all_char = soup.find_all("h3")
        title = all_char[1].text
        isbn = all_char[-2].text.split(": ")[-1]
        img = soup.find_all("table")[1].find("img").get("src")
        img = f"http://primuzee.ru/files/{img.split('/')[-1]}"
        book_data = {
            "Ссылка": link,
            "Название": title,
            "ISBN": isbn,
            "Изображение": img,
        }
        result.append(book_data)
    except Exception as e:
        logger.exception(link)
    finally:
        print(f"\rDone - {count}", end="")
        count += 1


async def get_cat_data(category):
    # async with session.get(category[0], headers=headers) as resp:
    resp = requests.get(category[0], headers=headers)
    soup = bs(resp.text, "lxml")
    print(category)
    try:
        paginator = soup.find("center").find_all("a")
    except AttributeError as e:
        print(e)
    if len(paginator) <= 1:
        paginator = 1
    else:
        paginator = paginator[-2].text
    for i in range(0, int(paginator)):
        # async with session.get(f"{BASE_URL}/{i}?tag={category[1]}") as resp:
        resp = requests.get(f"{BASE_URL}/{i}?tag={category[1]}", headers=headers)
        soup = bs(resp.text, "lxml")
        try:
            all_books = soup.find_all("table")[1].find_all("tr", {"height": False})
            all_book_links = [
                f"{BASE_URL}/{i.find("a").get("href").split("/")[-1]}"
                for i in all_books
            ]

            for link in all_book_links:
                get_item_data(link)
            # with ThreadPoolExecutor(max_workers=10) as executor:
            #     threads = [
            #         executor.submit(get_item_data, link) for link in all_book_links
            #     ]

        except IndexError as e:
            logger.exception(e)


async def get_gather_data():
    # async with aiohttp.ClientSession(
    #     connector=aiohttp.TCPConnector(ssl=False, limit=1), headers=headers
    # ) as session:
    #     async with session.get(BASE_URL) as resp:
    resp = requests.get(BASE_URL, headers=headers)
    soup = bs(resp.text, "lxml")
    all_cat = soup.find("ul", class_="books-list").find_all("li")
    all_cat = [(i.find("a").get("href"), i.text) for i in all_cat]

    tasks = [await get_cat_data(i) for i in all_cat]

    df = pd.DataFrame(result)
    df.to_excel("primuzee.xlsx", index=False)


def main():
    asyncio.run(get_gather_data())


if __name__ == "__main__":
    main()
