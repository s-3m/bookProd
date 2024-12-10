import json
import re
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from pprint import pprint
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd


headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    # 'Cookie': 'mdk_session=u258615rb5b6de9s23usk0k6oq; ab=423e39551c447a02edec1afdbad60a8fa3ba3871%7ES; city_zip=3a63c8786b135cd844d7071e992508e361acac14%7E101000; _ym_uid=1731307959487657928; _ym_d=1731307959; _gid=GA1.2.1239579180.1731307960; _ym_visorc=w; _ym_isad=2; _ga_V7RS373QY7=GS1.1.1731307959.1.1.1731309048.0.0.0; _ga=GA1.1.2130916544.1731307960',
    "Referer": "https://mdk-arbat.ru/catalog/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

num_list = [i for i in range(1, 10000)]
# num_list = [10000, 2]
links_list = []

semaphore = asyncio.Semaphore(10)


async def some_info(session, num):
    link = f"https://mdk-arbat.ru/catalog/?subj_id={num}"
    async with semaphore:
        async with session.get(link) as response:
            soup = bs(await response.text(), "lxml")
            # select_area = soup.find("span", {"class": "tg-select"})
            h1 = soup.find("h1").text.strip()
            if h1 != "Каталог | Московский Дом книги" and h1 != "-":
                cat_link = link.split("ru")[-1]
                links_list.append(cat_link)
                return


async def main():
    async with aiohttp.ClientSession(
        headers=headers, connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        tasks = []
        for num in num_list:
            task = asyncio.create_task(some_info(session, num))
            tasks.append(task)
        await asyncio.gather(*tasks)
    print(links_list)
    print(len(links_list))
    a = json.dumps(links_list, ensure_ascii=False, indent=4)
    with open("aaa.json", "w", encoding="utf-8") as f:
        f.write(a)


if __name__ == "__main__":
    asyncio.run(main())
