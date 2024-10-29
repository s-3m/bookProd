import time
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from selenium_data import get_book_data
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://www.moscowbooks.ru/"
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}
count = 1
result = []


def get_item_data(item):
    global count
    res_dict = {}

    link = f"{BASE_URL}{item}" if not item.startswith("/") else f"{BASE_URL}{item[1:]}"
    try:
        quantity, page_source = get_book_data(link)

        articul = item.split("/")[-2]
        isbn = "Нет ISBN"
        soup = bs(page_source, "lxml")

        all_details = soup.find_all("dl", class_="book__details-item")
        for detail in all_details:
            detail = detail.find_all("dt")
            if detail[0].text.strip() == "ISBN:":
                isbn = detail[1].text.strip()

        res_dict["Артикул"] = articul
        res_dict["ISBN"] = isbn
        res_dict["Наличие"] = quantity

        print(f"\r{count}", end="")
        count = count + 1

        return res_dict
    except:
        with open("erorr.txt", "a+") as file:
            file.write(link + "\n")
        return None


async def get_page_data(items):
    # futures = [asyncio.to_thread(get_item_data, item, main_category) for item in items]
    # for i in futures:
    #     result.append(await i)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_item_data, item) for item in items]
        result_future = [f.result() for f in futures if f.result() is not None]
    for i in result_future:
        result.append(i)


async def get_gather_data():
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False), trust_env=True
    ) as session:
        response = await session.get(f"{BASE_URL}/books/", headers=headers)
        response_text = await response.text()
        soup = bs(response_text, "lxml")
        max_pagination = soup.find("ul", class_="pager__list").find_all("li")[-2].text
        tasks = []
        for page in range(1, int(max_pagination) + 1):
            try:
                page_response = await session.get(
                    f"{BASE_URL}/books/?page={page}", headers=headers
                )
                page_html = await page_response.text()
                soup = bs(page_html, "lxml")
                all_books_on_page = soup.find_all("div", class_="catalog__item")
                all_items = [book.find("a")["href"] for book in all_books_on_page]
                task = asyncio.create_task(get_page_data(all_items[:5]))
                tasks.append(task)
            except Exception as e:
                with open("erorr.txt", "a+") as file:
                    file.write(f"{page} + - + {e}")

        await asyncio.gather(*tasks)


def main():
    asyncio.run(get_gather_data())
    df = pd.DataFrame(result)
    df.to_excel("result.xlsx", index=False)


if __name__ == "__main__":
    a = time.time()
    main()
    print(time.time() - a)
