import time
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger

BASE_URL = "https://www.labirint-bookstore.ru"
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en;q=0.9",
    "cache-control": "max-age=0",
    # 'cookie': 'fuckbot=0.48621060127121085; PHPSESSID=m1l1sp1s2a2jh4ej5m2rrev3e5; UserSes=ce59f76fbca1b65e5c5f169d0154882c01e99f84e6d277b24f; CLIENT_ID=0; __utma=158063783.364369771.1742049039.1742049039.1742049039.1; __utmc=158063783; __utmz=158063783.1742049039.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; _gid=GA1.2.108748407.1742049039; __utmb=158063783.8.10.1742049039; _ga=GA1.2.351912949.1742049039; _ga_2N9PCD90C3=GS1.1.1742049039.1.1.1742049326.0.0.0',
    "if-modified-since": "Mon, 03 Mar 2025 11:00:12 GMT",
    "priority": "u=0, i",
    "referer": "https://www.labirint-bookstore.ru/shops/310/",
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
cookies = {
    "fuckbot": "0.48621060127121085",
    "PHPSESSID": "m1l1sp1s2a2jh4ej5m2rrev3e5",
    "UserSes": "ce59f76fbca1b65e5c5f169d0154882c01e99f84e6d277b24f",
    "__utma": "158063783.364369771.1742049039.1742049039.1742049039.1",
    "__utmc": "158063783",
    "__utmz": "158063783.1742049039.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)",
    "_gid": "GA1.2.108748407.1742049039",
    "CLIENT_ID": "0",
    "_gat_UA-3229737-8": "1",
    "__utmt": "1",
    "_ga_2N9PCD90C3": "GS1.1.1742049039.1.1.1742051891.0.0.0",
    "_ga": "GA1.2.351912949.1742049039",
    "__utmb": "158063783.86.10.1742049039",
}

result = []
count = 1


async def get_item_data(session, link):
    global count
    try:
        async with session.get(f"{BASE_URL}{link}", headers=headers) as response:

            soup = bs(await response.text(), "lxml")
            available = soup.find("div", id="shops-available")
            if not available:
                return
            title = soup.find("h1").text.strip()
            img = soup.find("img", class_="img-cover-book").get("src")
            book_res = {"Название": title, "Фото": img}
            shop_area = soup.find_all("div", class_="shop-name")
            for shop in shop_area:
                shop_name = shop.text.strip().split("- ")[-1]
                book_res[shop_name] = "да"
            result.append(book_res)
    except Exception as e:
        logger.exception(e)
    finally:
        print(f"\rDone - {count}", end="")
        count += 1


async def get_gather_data():
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit=3),
        headers=headers,
        cookies=cookies,
    ) as session:
        for i in range(1, 2051):
            async with session.get(f"{BASE_URL}/books/?page={i}") as resp:
                soup = bs(await resp.text(), "lxml")
                all_products_on_page = soup.find(
                    "div", class_="books-catalog mt20"
                ).find_all("div", class_="books-name")
                all_links_on_page = [
                    book.find("a").get("href") for book in all_products_on_page
                ]

            tasks = [
                asyncio.create_task(get_item_data(session, link))
                for link in all_links_on_page
            ]
            await asyncio.gather(*tasks)


def main():
    asyncio.run(get_gather_data())
    df = pd.DataFrame(result)
    df.to_excel("labirint.xlsx", index=False)


if __name__ == "__main__":
    start = time.time()
    main()
    print()
    print((time.time() - start) / 60)
