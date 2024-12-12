import asyncio
import sys
import os
from loguru import logger
import aiohttp
import pandas as pd
from pandas.io.formats import excel
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import time

from bb.main import sample
from selenium_data import get_book_data
from tg_sender import tg_send_files

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import give_me_sample, fetch_request


DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.moscowbooks.ru/"
USER_AGENT = UserAgent()
PATH_TO_FILES = "/media/source/msk/every_day" if not DEBUG else "source/every_day"
logger.add(
    f"{PATH_TO_FILES}/error.log", format="{time} {level} {message}", level="ERROR"
)
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}

excel.ExcelFormatter.header_style = None
count = 1


async def to_check_item(item, session):
    global count
    link = f"{BASE_URL}/book/{item["article"][:-2]}"
    try:
        response = await fetch_request(session, link, headers=headers, sleep=None)
        soup = bs(response, "lxml")
        age_control = soup.find("input", id="age_verification_form_mode")
        script_index = 1
        if age_control:
            closed_page = get_book_data(link)
            soup = bs(closed_page, "lxml")
            script_index = 5

        if not soup.find("div", class_="book__buy"):
            item["stock"] = "del"
        else:
            need_element = soup.find_all("script")
            a = (
                need_element[script_index]
                .text.split("MbPageInfo = ")[1]
                .replace("false", "False")
                .replace("true", "True")
            )
            need_data_dict = eval(a[:-1])["Products"][0]
            stock = need_data_dict["Stock"]
            item["stock"] = stock

        print(f"\r{count}", end="")
        count += 1
    except Exception as e:
        item["stock"] = "error"
        logger.exception(f"ERROR with {link}")
        with open(
            f"{PATH_TO_FILES}/error.txt",
            "a+",
        ) as f:
            f.write(f"{link} ------ {e}\n")


async def get_compare():
    sample = give_me_sample(base_dir=PATH_TO_FILES, prefix="msk", without_merge=True)

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False), trust_env=True
    ) as session:
        for item in sample:
            await to_check_item(item, session)

        print()  # empty print for clear info visualization
        logger.warning("Start reparse error")

        for item in sample:
            if item["stock"] == "error":
                await to_check_item(item, session)
    global count
    count = 1

    for item in sample:
        if item["stock"] == "error":
            item["stock"] = "del"

    logger.info("Preparing files for sending")
    df_result = pd.DataFrame(sample)
    df_without_del = df_result.loc[df_result["stock"] != "del"]
    without_del_path = f"{PATH_TO_FILES}/msk_new_stock.xlsx"
    df_without_del.to_excel(without_del_path, index=False)

    df_del = df_result.loc[df_result["stock"] == "del"][["article"]]
    del_path = f"{PATH_TO_FILES}/msk_del.xlsx"
    df_del.to_excel(del_path, index=False)

    logger.info("Start sending files")
    await tg_send_files([without_del_path, del_path], subject="Москва")


def main():
    logger.info("Start parsing Moscow")
    asyncio.run(get_compare())
    time.sleep(10)


def super_main():
    main()


if __name__ == "__main__":
    super_main()
