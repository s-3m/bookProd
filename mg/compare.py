import os.path
import time
import schedule
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd
from tg_sender import tg_send_files
from loguru import logger

pandas.io.formats.excel.ExcelFormatter.header_style = None

BASE_URL = "https://www.dkmg.ru"
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}

count = 1

PATH_TO_FILES = "/media/source/mg/every_day"
logger.add(
    f"{PATH_TO_FILES}/error.log", format="{time} {level} {message}", level="ERROR"
)


async def get_item_data(session, item, semaphore, sample, reparse=False):
    global count
    item_id = item["id"]
    if not item_id:
        item["stock"] = "del"
        return
    url = f"{BASE_URL}/tovar/{item_id}"
    try:
        async with semaphore:
            await asyncio.sleep(0.5)
            async with session.get(url, headers=headers) as response:
                soup = bs(await response.text(), "lxml")
                buy_btn = soup.find("a", class_="btn_red wish_list_btn add_to_cart")
                if not buy_btn:
                    item["stock"] = "del"
                else:
                    item["stock"] = "2"

                if reparse:
                    sample.append(item)

                print(f"\rDone - {count}", end="")
                # logger.info(f'\rDone - {count}\r', end='')
                count += 1
    except Exception as e:
        with open(f"{PATH_TO_FILES}/error.txt", "a+") as file:
            file.write(f"{item_id} --- {item['article']} --- {e}\n")


async def get_gather_data(semaphore, sample):
    tasks = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit_per_host=10, limit=50),
        timeout=aiohttp.ClientTimeout(total=1200),
    ) as session:
        for item in sample:
            task = asyncio.create_task(get_item_data(session, item, semaphore, sample))
            tasks.append(task)

        await asyncio.gather(*tasks)

        reparse_count = 0
        while os.path.exists(f"{PATH_TO_FILES}/error.txt") and reparse_count < 10:
            # print('\n------- Start reparse error ------')
            print()
            logger.info("\nStart reparse error")
            reparse_count += 1
            with open(f"{PATH_TO_FILES}/error.txt") as file:
                id_list = [
                    {"id": i.split(" --- ")[0], "article": i.split(" --- ")[1]}
                    for i in file.readlines()
                ]
                print(f"--- Quantity error - {len(id_list)}")
                os.remove(f"{PATH_TO_FILES}/error.txt")

            reparse_tasks = []

            for item in id_list:
                task = asyncio.create_task(
                    get_item_data(session, item, semaphore, sample, reparse=True)
                )
                reparse_tasks.append(task)

            await asyncio.gather(*reparse_tasks)

            global count
            count = 1


def main():
    # print('start\n')
    logger.info("Start MG parsing")
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    df = pd.read_excel(
        f"{PATH_TO_FILES}/gvardia_new_stock.xlsx", converters={"id": str}
    )
    df = df.where(df.notnull(), None)
    sample = df.to_dict("records")

    semaphore = asyncio.Semaphore(5)
    asyncio.run(get_gather_data(semaphore, sample))
    df_result = pd.DataFrame(sample)
    df_result.drop_duplicates(inplace=True, keep="last", subset="article")

    result_file = f"{PATH_TO_FILES}/all_result.xlsx"
    df_result.to_excel(result_file, index=False)

    df_del = df_result.loc[df_result["stock"] == "del"][["article"]]
    del_file = f"{PATH_TO_FILES}/gvardia_del.xlsx"
    df_del.to_excel(del_file, index=False)

    df_without_del = df_result.loc[df_result["stock"] != "del"]
    stock_file = f"{PATH_TO_FILES}/gvardia_new_stock.xlsx"
    df_without_del.to_excel(stock_file, index=False)
    global count
    count = 1
    time.sleep(10)
    print()
    logger.success("Parse end successfully")
    asyncio.run(tg_send_files([stock_file, del_file], subject="Гвардия"))


def super_main():
    schedule.every().day.at("03:30").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    start_time = time.time()
    super_main()
    print()
    print(time.time() - start_time)
