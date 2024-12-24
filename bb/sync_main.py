import os.path
import sys
import os
import time
import re
from concurrent.futures.thread import ThreadPoolExecutor

import pandas.io.formats.excel
import unicodedata
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger

from mg.mg import item_error

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import (
    filesdata_to_dict,
    check_danger_string,
    fetch_request,
    sync_fetch_request,
)
from filter import filtering_cover

pd.io.formats.excel.ExcelFormatter.header_style = None
DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://bookbridge.ru"
BASE_LINUX_DIR = "/media/source/bb" if not DEBUG else "source"
USER_AGENT = UserAgent()
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    # 'Cookie': 'prefers-color-scheme=dark; prefers-color-scheme=dark; prefers-color-scheme=dark; PHPSESSID=2xIwX2GXzhVpTJAT1f2rC0Ny1oslM6M8; ASPRO_MAX_USE_MODIFIER=Y; BITRIX_SM_GUEST_ID=2178509; BITRIX_SM_SALE_UID=057e0d4dc6004dc87bc4852bf87d3c3c; prefers-color-scheme=dark; _ym_debug=null; BITRIX_CONVERSION_CONTEXT_s1=%7B%22ID%22%3A2%2C%22EXPIRE%22%3A1735073940%2C%22UNIQUE%22%3A%5B%22conversion_visit_day%22%5D%7D; searchbooster_v2_user_id=lGgA7OJ3m-x4Oh0hg-yyi_aK28KTqYycQl7gmKgnzQs%7C11.24.11.52; ageCheckPopupRedirectUrl=%2Fv2-mount-input; BX_USER_ID=134a88f51bde6d579bbcc0b7db55598d; _ym_uid=1724661849534829824; _ym_d=1735030348; _ym_isad=2; MAX_VIEWED_ITEMS_s1=%7B%22106761%22%3A%5B%221735031436896%22%2C%222810754%22%5D%7D; BITRIX_SM_LAST_VISIT=24.12.2024%2012%3A10%3A45',
    "Referer": "https://bookbridge.ru/catalog/angliyskiy/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 YaBrowser/24.12.0.0 Safari/537.36",
    "sec-ch-ua": '"Chromium";v="130", "YaBrowser";v="24.12", "Not?A_Brand";v="99", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}


prices = filesdata_to_dict(f"{BASE_LINUX_DIR}/prices")

# df_price_one = prices["1"]
# df_price_two = prices["2"]
# df_price_three = prices["3"]

logger.add(
    f"{BASE_LINUX_DIR}/bb_error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
sample = filesdata_to_dict(f"{BASE_LINUX_DIR}/sale", combined=True)
not_in_sale = filesdata_to_dict(f"{BASE_LINUX_DIR}/not_in_sale", combined=True)

count = 1
empty_price_count = 1
result = []

id_to_del = set(sample.keys())
id_to_add = []
item_error = []
page_error = []


def to_write_file(temporary=False, final_result=False):
    filepath = f"{BASE_LINUX_DIR}/result"
    if temporary:
        df = pd.DataFrame(result)
        df.to_excel(
            f"{BASE_LINUX_DIR}/result/temporary/temp_result.xlsx",
            index=False,
        )
        return
    if not final_result:
        filepath = f"{BASE_LINUX_DIR}/result/temporary"
    df = pd.DataFrame(result)
    df.to_excel(f"{filepath}/bb_all.xlsx", index=False)

    for price_item in prices:
        df_result = pd.DataFrame().from_dict(prices[price_item], orient="index")
        df_result.index.name = "article"
        df_result.to_excel(f"{filepath}/bb_price_{price_item}.xlsx", index=True)

    df_not_in_sale = pd.DataFrame().from_dict(not_in_sale, orient="index")
    df_not_in_sale = df_not_in_sale.loc[df_not_in_sale["on sale"] == "да"][["article"]]
    df_not_in_sale.to_excel(f"{filepath}/bb_not_in_sale.xlsx")

    df_add = pd.DataFrame(id_to_add)
    df_add.to_excel(f"{filepath}/bb_add.xlsx", index=False)

    df_del = pd.DataFrame(id_to_del)
    df_del.columns = ["Артикул"]
    df_del.to_excel(f"{filepath}/bb_del.xlsx", index=False)


semaphore = asyncio.Semaphore(2)


def get_item_data(item, main_category=None):
    global count
    global semaphore
    res_dict = {}
    link = f"{BASE_URL}{item}"
    res_dict["Ссылка"] = link
    try:
        response = sync_fetch_request(link, headers=headers)
        # async with session.get(link, headers=headers) as response:
        #     await asyncio.sleep(10)
        #     soup = bs(await response.text(), "lxml")
        #
        # if soup.find("h1").text.strip() == "Service Temporarily Unavailable":
        #     await asyncio.sleep(500)
        #     async with session.get(link, headers=headers) as response:
        #         await asyncio.sleep(5)
        #         soup = bs(await response.text(), "html.parser")
        soup = bs(response, "lxml")

        if not main_category:
            main_category = (
                soup.find_all("meta", attrs={"itemprop": "category"})[0]
                .get("content")
                .split("/")[1]
            )

        try:
            title = soup.find("h1").text
            title = asyncio.run(check_danger_string(title, "title"))
            if not title:
                return
            res_dict["title"] = title.strip()
        except:
            title = "Нет названия"
            res_dict["title"] = title

        try:
            pattern = re.compile(r"setViewedProduct\((\d+, .+)'MIN_PRICE':([^'].+}),")
            script = soup.find("script", string=pattern)
            price = eval(pattern.search(script.text).group(2)).get("ROUND_VALUE_VAT")
            res_dict["price"] = price
        except:
            return

        res_dict["category"] = main_category

        try:
            article = (
                soup.find("div", class_="article").find_all("span")[1].text.strip()
            )
            res_dict["Артикул_OZ"] = article + ".0"
        except:
            return

        try:
            photo_link = soup.find(
                class_="detail-gallery-big-slider-main__ratio-inner"
            ).find("img")["src"]
            photo_path = BASE_URL + photo_link
            res_dict["photo"] = photo_path
        except:
            res_dict["photo"] = "https://zapobedu21.ru/images/26.07.2017/kniga.jpg"

        try:
            quantity = (
                soup.find(class_="shadowed-block")
                .find(class_="item-stock")
                .find(class_="value")
                .text.strip()
            )
            res_dict["quantity"] = quantity
        except:
            quantity = "Наличие не указано"
            res_dict["quantity"] = quantity

        try:
            desc = (
                soup.find(class_="ordered-block desc")
                .find(class_="content")
                .text.strip()
            )
            desc = asyncio.run(check_danger_string(desc, "description"))
            res_dict["description"] = desc
        except:
            res_dict["description"] = "Нет описания"

        try:
            all_chars = soup.find(class_="char_block").find("table").find_all("tr")
            for i in all_chars:
                char = i.find_all("td")
                res_dict[char[0].text.strip()] = char[1].text.strip()

        except:
            try:
                all_chars = soup.find_all("div", class_="properties-group__item")
                for char in all_chars:
                    char_tuple = char.find_all("div")
                    res_dict[char_tuple[0].text.strip()] = char_tuple[1].text.strip()
            except:
                pass

        # Cover filter
        cover_type = res_dict.get("Тип обложки")
        if cover_type:
            new_cover = filtering_cover(cover_type)
            if new_cover == "del":
                del res_dict["Тип обложки"]
            else:
                res_dict["Тип обложки"] = new_cover

        # Digit filter
        digit_item = res_dict.get("Вид продукта")
        if digit_item:
            if digit_item == "Цифровой":
                return

        # Author filter
        author = res_dict.get("Автор")
        if not author:
            res_dict["Автор"] = "Нет автора"

        # Publisher filter
        publisher = res_dict.get("Издательство")
        if not publisher:
            res_dict["Издательство"] = "Не указано"

        # ISBN filter
        isbn = res_dict.get("ISBN")
        if isbn:
            digit_checker = isbn.replace("_", "").isdigit()
            if not digit_checker:
                res_dict["ISBN"] = "978-5-0000-0000-0"

        for d in prices:
            if article + ".0" in prices[d] and quantity != "Нет в наличии":
                prices[d][article + ".0"]["price"] = price

        if article + ".0" in not_in_sale and quantity != "Нет в наличии":
            not_in_sale[article + ".0"]["on sale"] = "Да"
        elif article + ".0" not in sample and quantity != "Нет в наличии":
            res_dict["Артикул"] = article + ".0"
            id_to_add.append(res_dict)
        if article + ".0" in id_to_del and quantity != "Нет в наличии":
            id_to_del.remove(article + ".0")

        if count % 50 == 0:
            to_write_file(temporary=True)

        print(f"\rDone - {count}", end="")
        count = count + 1
        result.append(res_dict)

    except Exception as e:
        item_error.append(link)
        logger.exception(f"\n{'-' * 50}\nERROR with --- {link}\n{'-' * 50}")
        if item:
            with open(f"{BASE_LINUX_DIR}/error.txt", "a+", encoding="utf-8") as file:
                file.write(f"{item} --- {e}\n")
        pass


def get_price_data(item):
    global empty_price_count
    item_article = item["article"][:-2]
    url = f"{BASE_URL}/catalog/?q={item_article}"
    try:
        response = sync_fetch_request(url, headers)
        soup = bs(response, "lxml")
        div_list = soup.find_all("div", class_="inner_wrap TYPE_1")

        for div in div_list:
            div_article = str(div.find("div", class_="article_block").get("data-value"))
            div_stock = soup.find("div", class_="item-stock").text
            if div_article != item_article or div_stock == "Нет в наличии":
                continue
            div_price_value = div.find_all("span", class_="price_value")[-1]
            if div_price_value:
                price_value: str = unicodedata.normalize("NFKD", div_price_value.text)
                item["price"] = price_value.replace(" ", "")

        print(f"\r{empty_price_count}", end="")
        empty_price_count += 1
    except Exception as e:
        logger.exception(f"Exception in reparse price")


async def check_empty_price():

    empty_price = []
    df_empty_price_one = pd.read_excel(
        f"{BASE_LINUX_DIR}/result/price_1.xlsx",
        converters={"article": str, "price": str},
    )
    df_empty_price_one = df_empty_price_one.where(df_empty_price_one.notnull(), None)
    price_one = df_empty_price_one.to_dict(orient="records")

    df_empty_price_two = pd.read_excel(
        f"{BASE_LINUX_DIR}/result/price_2.xlsx",
        converters={"article": str, "price": str},
    )
    df_empty_price_two = df_empty_price_two.where(df_empty_price_two.notnull(), None)
    price_two = df_empty_price_two.to_dict(orient="records")

    df_empty_price_three = pd.read_excel(
        f"{BASE_LINUX_DIR}/result/price_3.xlsx",
        converters={"article": str, "price": str},
    )
    df_empty_price_three = df_empty_price_three.where(
        df_empty_price_three.notnull(), None
    )
    price_three = df_empty_price_three.to_dict(orient="records")

    for item_price in (price_one, price_two, price_three):
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in item_price:
                if not i["price"]:
                    empty_price.append(i)
                    executor.submit(get_price_data, i)

    logger.info(f"Total empty price in PRICE_ONE - {len(empty_price)}")

    print()
    logger.info(f"Start wright files")

    pd.DataFrame(price_one).to_excel(
        f"{BASE_LINUX_DIR}/result/price_1.xlsx", index=False
    )
    pd.DataFrame(price_two).to_excel(
        f"{BASE_LINUX_DIR}/result/price_2.xlsx", index=False
    )
    pd.DataFrame(price_three).to_excel(
        f"{BASE_LINUX_DIR}/result/price_3.xlsx", index=False
    )


async def get_gather_data():
    all_need_links = []
    logger.info("Start to collect data")
    response = sync_fetch_request(f"{BASE_URL}/catalog", headers=headers)
    soup = bs(response, "lxml")
    all_lang = soup.find("div", class_="catalog_section_list").find_all("li")
    all_lang = [i.find("a")["href"] for i in all_lang]

    for lang in all_lang:
        try:
            response = sync_fetch_request(f"{BASE_URL}{lang}", headers=headers)
            soup = bs(response, "lxml")
            all_cat = soup.find_all("div", class_="section-compact-list__info")
            all_need_cat = [i.find("a")["href"] for i in all_cat]
            all_need_links.extend(all_need_cat)
        except:
            continue

    for link in all_need_links[:5]:
        response = sync_fetch_request(f"{BASE_URL}{link}", headers=headers)
        await asyncio.sleep(10)
        soup = bs(response, "lxml")

        pagination = soup.find("div", class_="nums")
        if pagination:
            pagination = int(pagination.find_all("a")[-1].text.strip())
        else:
            pagination = 1
        pagination = 2

        for page in range(1, pagination + 1):
            await asyncio.sleep(2)

            try:
                for _ in range(20):
                    response = sync_fetch_request(
                        f"{BASE_URL}{link}?PAGEN_1={page}", headers=headers
                    )
                    await asyncio.sleep(2)
                    soup = bs(response, "lxml")
                    page_items = soup.find_all("div", class_="item-title")
                    items = [item.find("a")["href"] for item in page_items]
                    main_category = soup.find("h1").text.strip()

                    with ThreadPoolExecutor(max_workers=10) as executor:
                        for item in items:
                            executor.submit(get_item_data, item, main_category)
                    break
                else:
                    await asyncio.sleep(10)
            except Exception as e:
                page_error.append(f"{BASE_URL}{link}?PAGEN_1={page}")
                with open(
                    f"{BASE_LINUX_DIR}/page_error.txt", "a+", encoding="utf-8"
                ) as file:
                    file.write(f"{BASE_URL}{link} --- {page} --- {e}\n")
                continue

    print()  # empty print for break string after count
    logger.success("Main data was collected")
    to_write_file()
    logger.success("The main data was written to files")

    # Собираем страницы с ошибками
    try:
        if page_error:
            logger.warning(f"Find page error - {len(page_error)}")
            for i in page_error:
                try:
                    response = sync_fetch_request(i, headers)
                    soup = bs(response, "lxml")
                    page_items = soup.find_all("div", class_="item-title")
                    items = [item.find("a")["href"] for item in page_items]
                    main_category = soup.find("h1").text.strip()

                    with ThreadPoolExecutor(max_workers=5) as executor:
                        for item in items:
                            executor.submit(get_item_data, item, main_category)
                except Exception as e:
                    logger.exception(f"EXCEPTION in {i}")

            to_write_file()
            logger.info("Pages error data was collected")
    except:
        logger.exception("Error in reparse pages errors")

    logger.warning("Start reparse items errors")

    # Собираем ошибки по отдельным книгам
    if item_error:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for item in item_error:
                executor.submit(get_item_data, item, None)

    to_write_file(final_result=True)

    logger.info("Start check empty price field")
    # await check_empty_price()

    logger.success("All done successfully!!!")


@logger.catch
def main():
    asyncio.run(get_gather_data())


if __name__ == "__main__":
    a = time.time()
    main()
    print()
    print(time.time() - a)
