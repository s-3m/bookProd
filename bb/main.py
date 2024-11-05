import os.path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import time
import re
import pandas.io.formats.excel
import unicodedata
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger

from utils import filesdata_to_dict

pd.io.formats.excel.ExcelFormatter.header_style = None

BASE_URL = "https://bookbridge.ru"
# BASE_LINUX_DIR = "/media/source/bb"
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}
df_price_one, df_price_two, df_price_three = filesdata_to_dict(
    "../source/Букбридж/Букбридж_цены"
)
sample = filesdata_to_dict("../source/Букбридж/Букбридж/В продаже", combined=True)
not_in_sale = filesdata_to_dict(
    "../source/Букбридж/Букбридж/Не в продаже", combined=True
)

count = 1
empty_price_count = 1
result = []

id_to_del = []
id_to_add = []


def to_write_file(filepath, temporary=False, final_result=False):
    if temporary:
        df = pd.DataFrame(result)
        df.to_excel(f"{filepath}.xlsx", index=False)
        return
    if not final_result:
        filepath = filepath + "/temporary"
    df = pd.DataFrame(result)
    df.to_excel(f"{filepath}/all_result.xlsx", index=False)

    df_one = pd.DataFrame().from_dict(df_price_one, orient="index")
    df_one.index.name = "article"
    df_one.to_excel(f"{filepath}/price_one.xlsx", index=True)

    df_two = pd.DataFrame().from_dict(df_price_two, orient="index")
    df_two.index.name = "article"
    df_two.to_excel(f"{filepath}/price_two.xlsx")

    df_three = pd.DataFrame().from_dict(df_price_three, orient="index")
    df_three.index.name = "article"
    df_three.to_excel(f"{filepath}/price_three.xlsx")

    df_not_in_sale = pd.DataFrame().from_dict(not_in_sale, orient="index")
    df_not_in_sale.index.name = "article"
    df_not_in_sale.to_excel(f"{filepath}/not_in_sale.xlsx")

    df_add = pd.DataFrame(id_to_add)
    df_add.to_excel(f"{filepath}/add.xlsx", index=False)

    df_del = pd.DataFrame(id_to_del)
    df_del.to_excel(f"{filepath}/del.xlsx", index=False)


semaphore = asyncio.Semaphore(10)


async def get_item_data(item, session, main_category=None):
    global count
    global semaphore
    res_dict = {}
    link = f"{BASE_URL}{item}"
    res_dict["link"] = link
    await asyncio.sleep(5)
    try:
        async with semaphore:
            async with session.get(link, headers=headers) as response:
                await asyncio.sleep(10)
                soup = bs(await response.text(), "lxml")

            if soup.find("h1").text.strip() == "Service Temporarily Unavailable":
                await asyncio.sleep(500)
                async with session.get(link, headers=headers) as response:
                    await asyncio.sleep(5)
                    soup = bs(await response.text(), "html.parser")

        if not main_category:
            main_category = soup.find_all(
                "span", class_="breadcrumbs__item-name font_xs"
            )[1].text.strip()

        try:
            title = soup.find("h1").text
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
            price = "Цена не указана"
            res_dict["price"] = price

        res_dict["category"] = main_category

        try:
            article = (
                soup.find("div", class_="article").find_all("span")[1].text.strip()
            )
            res_dict["article"] = article + ".0"
        except:
            article = "Нет артикула"
            res_dict["article"] = article

        try:
            photo_link = soup.find(class_="product-detail-gallery__picture")["data-src"]
            photo_path = BASE_URL + photo_link
            res_dict["photo"] = photo_path
        except:
            res_dict["photo"] = "Нет фото"

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
                all_chars = soup.find(class_="product-chars").find_all(
                    class_="properties__item"
                )
                for i in all_chars:
                    res_dict[i.find(class_="properties__title").text.strip()] = i.find(
                        class_="properties__value"
                    ).text.strip()
            except:
                pass

        for d in [df_price_one, df_price_two, df_price_three]:
            if article + ".0" in d:
                d[article + ".0"]["price"] = price
                break

        if article + ".0" in not_in_sale and quantity != "Нет в наличии":
            not_in_sale[article + ".0"]["on sale"] = "Да"
        if article + ".0" not in sample and quantity != "Нет в наличии":
            res_dict["article"] = article + ".0"
            id_to_add.append(res_dict)
        elif article + ".0" in sample and quantity == "Нет в наличии":
            id_to_del.append({"article": article + ".0"})

        if count % 50 == 0:
            to_write_file(
                filepath="result/temporary/temporary_result.xlsx", temporary=True
            )

        print(f"\rDone - {count}", end="")
        count = count + 1
        result.append(res_dict)

    except Exception as e:
        if item.strip():
            with open("error_log.txt", "a+", encoding="utf-8") as file:
                file.write(f"{item} --- {e}\n")
        pass


async def get_price_data(item, session, semaphore_price):
    global empty_price_count
    item_article = item["article"][:-2]
    url = f"{BASE_URL}/catalog/?q={item_article}"
    async with semaphore_price:
        async with session.get(url, headers=headers) as resp:
            await asyncio.sleep(5)
            soup = bs(await resp.text(), "html.parser")
            div_list = soup.find_all("div", class_="inner_wrap TYPE_1")

            for div in div_list:
                div_article = str(
                    div.find("div", class_="article_block").get("data-value")
                )
                div_stock = soup.find("div", class_="item-stock").text
                if div_article != item_article or div_stock == "Нет в наличии":
                    continue
                div_price_value = div.find("span", class_="price_value")
                if div_price_value:
                    price_value: str = unicodedata.normalize(
                        "NFKD", div_price_value.text
                    )
                    item["price"] = price_value.replace(" ", "")

            print(f"\r{empty_price_count}", end="")
            empty_price_count += 1


async def check_empty_price(session):
    semaphore_price = asyncio.Semaphore(5)

    empty_price_tasks = []
    df_empty_price_one = pd.read_excel(
        "result/price_one.xlsx", converters={"article": str, "price": str}
    )
    df_empty_price_one = df_empty_price_one.where(df_empty_price_one.notnull(), None)
    price_one = df_empty_price_one.to_dict(orient="records")

    df_empty_price_two = pd.read_excel(
        "result/price_two.xlsx", converters={"article": str, "price": str}
    )
    df_empty_price_two = df_empty_price_two.where(df_empty_price_two.notnull(), None)
    price_two = df_empty_price_two.to_dict(orient="records")

    df_empty_price_three = pd.read_excel(
        "result/price_three.xlsx", converters={"article": str, "price": str}
    )
    df_empty_price_three = df_empty_price_three.where(
        df_empty_price_three.notnull(), None
    )
    price_three = df_empty_price_three.to_dict(orient="records")

    for item_price in (price_one, price_two, price_three):
        for i in item_price:
            if not i["price"]:
                task = asyncio.create_task(get_price_data(i, session, semaphore_price))
                empty_price_tasks.append(task)
    logger.info(f"Total empty price in PRICE_ONE - {len(empty_price_tasks)}")
    await asyncio.gather(*empty_price_tasks)

    print()
    logger.info(f"Start wright files")

    pd.DataFrame(price_one).to_excel("result/price_one.xlsx", index=False)
    pd.DataFrame(price_two).to_excel("result/price_two.xlsx", index=False)
    pd.DataFrame(price_three).to_excel("result/price_three.xlsx", index=False)


async def get_gather_data():
    all_need_links = []
    logger.info("Start to collect data")
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit=50, limit_per_host=10),
        trust_env=True,
    ) as session:
        response = await session.get(f"{BASE_URL}/catalog", headers=headers)
        response_text = await response.text()
        soup = bs(response_text, "lxml")
        all_lang = soup.find("div", class_="catalog_section_list").find_all("li")
        all_lang = [i.find("a")["href"] for i in all_lang]

        for lang in all_lang:
            try:
                response = await session.get(f"{BASE_URL}{lang}", headers=headers)
                soup = bs(await response.text(), "lxml")
                all_cat = soup.find_all("div", class_="section-compact-list__info")
                all_need_cat = [i.find("a")["href"] for i in all_cat]
                all_need_links.extend(all_need_cat)
            except:
                continue

        tasks = []
        for link in all_need_links:
            response = await session.get(f"{BASE_URL}{link}", headers=headers)
            await asyncio.sleep(10)
            soup = bs(await response.text(), "lxml")

            pagination = soup.find("div", class_="nums")
            if pagination:
                pagination = int(pagination.find_all("a")[-1].text.strip())
            else:
                pagination = 1
            # pagination = 3
            for page in range(1, pagination + 1):
                await asyncio.sleep(5)

                try:
                    async with session.get(
                        f"{BASE_URL}{link}?PAGEN_1={page}", headers=headers
                    ) as response:
                        await asyncio.sleep(10)
                        soup = bs(await response.text(), "html.parser")
                        page_items = soup.find_all("div", class_="item-title")
                        items = [item.find("a")["href"] for item in page_items]
                        main_category = soup.find("h1").text.strip()

                    for item in items:
                        task = asyncio.create_task(
                            get_item_data(item, session, main_category)
                        )
                        tasks.append(task)
                except Exception as e:
                    with open("page_error.txt", "a+", encoding="utf-8") as file:
                        file.write(f"{link} --- {page} --- {e}\n")

        await asyncio.gather(*tasks)
        await asyncio.sleep(10)
        print()  # empty print for break string after count
        logger.success("Main data was collected")
        to_write_file("result")
        logger.success("The main data was written to files")

        logger.warning("Start reparse error")

        reparse_tasks = []
        reparse_count = 0
        while os.path.exists("error_log.txt") and reparse_count < 7:
            with open("error_log.txt", encoding="utf-8") as file:
                reparse_items = file.readlines()
                reparse_items = [
                    i.split(" -")[0].strip() for i in reparse_items if i.strip()
                ]
            logger.info(f"Total error reparse - {len(reparse_items)}")
            os.remove("error_log.txt")
            for item in reparse_items:
                task = asyncio.create_task(get_item_data(item, session))
                reparse_tasks.append(task)
            reparse_count += 1
            await asyncio.gather(*reparse_tasks)

        to_write_file(filepath="result", final_result=True)

        logger.info("Start check empty price field")
        await check_empty_price(session)

        logger.success("All done successfully!!!")


def main():
    asyncio.run(get_gather_data())


if __name__ == "__main__":
    a = time.time()
    main()
    print()
    print(time.time() - a)
