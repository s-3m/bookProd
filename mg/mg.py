import time
import sys
import os
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from pprint import pprint
from fake_useragent import UserAgent
import aiohttp
import asyncio
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import (
    filesdata_to_dict,
    check_danger_string,
    fetch_request,
    write_result_files,
)

pandas.io.formats.excel.ExcelFormatter.header_style = None
DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.dkmg.ru"
BASE_LINUX_DIR = "/media/source/mg" if not DEBUG else "source"
logger.add(
    f"{BASE_LINUX_DIR}/error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}

prices = filesdata_to_dict(f"{BASE_LINUX_DIR}/prices")

sample = filesdata_to_dict(f"{BASE_LINUX_DIR}/sale", combined=True)
not_in_sale = filesdata_to_dict(f"{BASE_LINUX_DIR}/not_in_sale", combined=True)

result = []
id_to_add = []
id_to_del = []

semaphore = asyncio.Semaphore(20)


async def get_item_data(session, link, main_category):
    link = f"{BASE_URL}{link}"
    global semaphore
    try:
        item_data = {}
        async with semaphore:
            response = await fetch_request(session, link, headers)
            soup = bs(response, "lxml")
            item_data["category"] = main_category
            try:
                title = soup.find("h1").text.strip()
                title = await check_danger_string(title, "title")
                if not title:
                    return
                item_data["title"] = title
            except:
                item_data["title"] = "Нет названия"
            try:
                options = soup.find("div", class_="item_basket_cont").find_all("tr")
                for option in options:
                    item_data[option.find_all("td")[0].text.strip()] = option.find_all(
                        "td"
                    )[1].text.strip()
                    if option.find_all("td")[0].text.strip() == "ISBN:":
                        isbn = option.find_all("td")[1].text.strip()
                try:
                    additional_options = soup.find(
                        "div", class_="additional_information"
                    ).find_all("tr")
                    for option in additional_options:
                        item_data[option.find_all("td")[0].text.strip()] = (
                            option.find_all("td")[1].text.strip()
                        )
                except:
                    pass
            except:
                item_data["Характеристика"] = "Характиристики не указаны"
            try:
                info = soup.find("div", class_="content_sm_2").find("h4")
                if info.text.strip() == "Аннотация":
                    info = info.find_next().text.strip()
                else:
                    info = "Описание отсутствует"
                info = await check_danger_string(info, "description")
                item_data["description"] = info
            except:
                item_data["description"] = "Описание отсутствует"
            try:
                price = (
                    soup.find_all("div", class_="product_item_price")[1]
                    .text.strip()
                    .split(".")[0]
                )
                item_data["price"] = price
            except:
                item_data["price"] = "Цена не указана"

            item_id = soup.find("div", class_="wish_list_btn_box").find(
                "a", class_="btn_desirable2 to_wishlist"
            )
            if item_id:
                item_id = item_id["data-tovar"]
                item_data["id"] = item_id
            try:
                quantity = soup.find("div", class_="wish_list_poz").text.strip()
                item_data["quantity"] = quantity
            except:
                item_data["quantity"] = "Наличие не указано"
            try:
                photo = soup.find("a", class_="highslide")["href"]
                item_data["photo"] = BASE_URL + photo
            except:
                item_data["photo"] = "Нет изображения"

            if isbn + ".0" in not_in_sale and quantity == "есть в наличии":
                not_in_sale[isbn + ".0"]["on sale"] = "да"
            elif isbn + ".0" not in sample and quantity == "есть в наличии":
                id_to_add.append(item_data)
            elif isbn + ".0" in sample and quantity != "есть в наличии":
                id_to_del.append({"article": f"{isbn}.0"})

            for d in prices:
                if isbn + ".0" in prices[d] and quantity == "есть в наличии":
                    prices[d][isbn + ".0"]["price"] = price

            result.append(item_data)
    except Exception as e:
        logger.exception(link)
        with open(f"{BASE_LINUX_DIR}/error.txt", "a+", encoding="utf-8") as f:
            f.write(f"{link} ----- {e}\n")


async def get_gather_data():
    tasks = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        response = await session.get(BASE_URL, headers=headers)
        response_text = await response.text()
        soup = bs(response_text, "lxml")
        cat_list = soup.find_all("h4")
        cat_list = [item.find("a")["href"] for item in cat_list[:8]]

        for cat_link in cat_list:
            try:
                url = BASE_URL + cat_link
                # response = await session.get(BASE_URL + cat_link, headers=headers)
                response = await fetch_request(session, url, headers)
                soup = bs(response, "lxml")
                pagin_max = int(
                    soup.find("div", class_="navitem")
                    .find_all("a")[-2]["href"]
                    .split("=")[-1]
                )
                main_category = soup.find("h1").text.split(" (")[0]
                logger.info(f"\n---Делаю категорию - {main_category}---")

                for page_numb in range(1, pagin_max + 1):
                    logger.info(
                        f"----------------стр - {page_numb} из {pagin_max}-----------"
                    )
                    # response = await session.get(
                    #     f"{BASE_URL}{cat_link}?page={page_numb}&orderNew=asc"
                    # )
                    page_url = f"{BASE_URL}{cat_link}?page={page_numb}&orderNew=asc"
                    response = await fetch_request(session, page_url, headers)
                    soup = bs(response, "lxml")
                    items_on_page = soup.find_all("div", class_="product_img")
                    items_links = [item.find("a")["href"] for item in items_on_page]
                    for link in items_links:
                        task = asyncio.create_task(
                            get_item_data(session, link, main_category)
                        )
                        tasks.append(task)
            except Exception as e:
                with open(f"{BASE_LINUX_DIR}/cat_error.txt", "a+") as f:
                    f.write(f"{BASE_URL}{cat_link} ----- {e}\n")
                    continue
        await asyncio.gather(*tasks)


def main():
    logger.info("Start parsing Gvardia")
    asyncio.run(get_gather_data())
    logger.info("Finish parsing Gvardia")
    logger.info("Start to write to excel")

    write_result_files(
        base_dir=BASE_LINUX_DIR,
        prefix="mg",
        all_books_result=result,
        id_to_add=id_to_add,
        id_to_del=id_to_del,
        not_in_sale=not_in_sale,
        prices=prices,
    )

    logger.info("Finish to write to excel")
    logger.success("Gvardia pars success")


if __name__ == "__main__":
    start_time = time.time()
    main()
    pprint(time.time() - start_time)
