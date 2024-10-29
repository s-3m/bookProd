import os
import time
import datetime
from dotenv import load_dotenv
import schedule
from loguru import logger
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd
from tg_sender import tg_send_files

pandas.io.formats.excel.ExcelFormatter.header_style = None

BASE_URL = "https://bookbridge.ru"
USER_AGENT = UserAgent()

headers = {
    'Accept': '*/*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'BX-ACTION-TYPE': 'get_dynamic',
    'BX-CACHE-BLOCKS': '{"4i19eW":"68b329da9893","basketitems-component-block":"d41d8cd98f00","header-auth-block1":"d41d8cd98f00","mobile-basket-with-compare-block1":"d41d8cd98f00","header-auth-block2":"d41d8cd98f00","header-basket-with-compare-block1":"d41d8cd98f00","header-auth-block3":"d41d8cd98f00","header-basket-with-compare-block2":"d41d8cd98f00","header-basket-with-compare-block3":"d41d8cd98f00","header-auth-block4":"d41d8cd98f00","mobile-auth-block1":"d41d8cd98f00","mobile-basket-with-compare-block2":"d41d8cd98f00","dv_351631":"87e7cc8bdbc9","qepX1R":"d41d8cd98f00","OhECjo":"d41d8cd98f00","6zLbbW":"c40340d595f5","KSBlai":"d41d8cd98f00","area":"d41d8cd98f00","des":"d41d8cd98f00","viewed-block":"d41d8cd98f00","footer-subscribe":"d41d8cd98f00","8gJilP":"d41d8cd98f00","basketitems-block":"d41d8cd98f00","bottom-panel-block":"d41d8cd98f00"}',
    'BX-CACHE-MODE': 'HTMLCACHE',
    'BX-REF': 'https://bookbridge.ru/catalog/angliyskiy/uchebnaya_literatura/',
    'Connection': 'keep-alive',
    # 'Cookie': 'ASPRO_MAX_USE_MODIFIER=Y; BITRIX_SM_GUEST_ID=1624218; BITRIX_SM_SALE_UID=e18c295fc8063d2ca6e15168ee6ac63d; _ym_debug=null; BITRIX_CONVERSION_CONTEXT_s1=%7B%22ID%22%3A2%2C%22EXPIRE%22%3A1727125140%2C%22UNIQUE%22%3A%5B%22conversion_visit_day%22%5D%7D; _ym_uid=1727085555223271303; _ym_isad=2; BX_USER_ID=7fb6960376433edd08736e7dacc8660d; PHPSESSID=sKj7Sj7tTeow2NuxAqVXp1BzIu3A2nTq; _ym_visorc=w; MAX_VIEWED_ITEMS_s1=%7B%2218139%22%3A%5B%221727085604307%22%2C%222106313%22%5D%2C%2277316%22%3A%5B%221727089427922%22%2C%222212995%22%5D%2C%22351631%22%3A%5B%221727089643749%22%2C%222215984%22%5D%7D; _ym_d=1727089644; BITRIX_SM_LAST_VISIT=23.09.2024%2014%3A07%3A24',
    'Referer': 'https://bookbridge.ru/catalog/angliyskiy/detskaya_literatura_1/razvivayushchaya_literatura_dlya_detey/knigi_dlya_uchashchikhsya_nachalnoy_shkoly_7_10_let_3/351631/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'X-Bitrix-Composite': 'get_dynamic',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

count = 1


async def get_item_data(session, item, error_items, semaphore):
    async with semaphore:
        try:
            async with session.get(item['link'], headers=headers) as resp:
                await asyncio.sleep(3)
                response = await resp.json(content_type=None)
                dynamic_block = response.get('dynamicBlocks')
                if not dynamic_block:
                    item['in_stock'] = 'del'
                    return
                page_text = dynamic_block[12]['CONTENT'].strip()
                soup = bs(page_text, 'html.parser')
                quantity_element = soup.find("span", class_="plus dark-color")
                stock_quantity = 'del'
                if quantity_element:
                    stock_quantity = quantity_element.get("data-max")
                global count
                print(f'\r{count}', end='')
                count += 1
            item["in_stock"] = stock_quantity
        except Exception as e:
            item['in_stock'] = '2'
            error_items.append(item)
            today = datetime.date.today().strftime('%d-%m-%Y')
            with open('error.txt', 'a+') as f:
                f.write(f"{today} --- {item['link']} --- {e}\n")


async def get_gather_data():
    df = pd.read_excel('compare/bb_new_stock_dev.xlsx', converters={'article': str})
    df = df.where(df.notnull(), None)
    all_items_list = df.to_dict('records')
    error_items_list = []
    semaphore = asyncio.Semaphore(5)
    tasks = []
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, limit=50, limit_per_host=10),
                                     trust_env=True) as session:
        for item in all_items_list:
            if not item['link']:
                item['in_stock'] = 'del'
                continue
            task = asyncio.create_task(get_item_data(session, item, error_items_list, semaphore))
            tasks.append(task)

        await asyncio.gather(*tasks)

        # Start reparse error
        error_tasks = []
        reparse_count = 0
        while error_items_list and reparse_count < 4:
            print()
            logger.warning('Start reparse error')
            reparse_count += 1
            new_items_list = error_items_list.copy()
            error_items_list.clear()
            for item in new_items_list:
                task = asyncio.create_task(get_item_data(session, item, error_items_list, semaphore))
                error_tasks.append(task)
            await asyncio.gather(*error_tasks)
            all_items_list.extend(new_items_list)

    print()
    logger.success('Finished parser successfully')
    global count
    count = 1

    await asyncio.sleep(30)
    logger.info('preparing files for sending')
    abs_path = os.path.abspath(os.path.dirname(__file__))
    df_result = pd.DataFrame(all_items_list)
    df_result.drop_duplicates(keep='last', inplace=True, subset='article')
    df_result.loc[df_result['in_stock'] != 'del'].to_excel(f'{abs_path}/compare/bb_new_stock_dev.xlsx', index=False)
    df_without_del = df_result.loc[df_result['in_stock'] != 'del'][['article', 'in_stock']]
    df_del = df_result.loc[df_result['in_stock'] == 'del'][['article']]
    del_path = f'{abs_path}/compare/bb_del.xlsx'
    without_del_path = f'{abs_path}/compare/bb_new_stock.xlsx'
    df_without_del.to_excel(without_del_path, index=False)
    df_del.to_excel(del_path, index=False)

    await asyncio.sleep(10)
    logger.info('Start sending files')
    await tg_send_files([without_del_path, del_path], subject='бб')


def main():
    logger.info('Start parsing BookBridge.ru')
    asyncio.run(get_gather_data())


def super_main():
    load_dotenv('../.env')
    schedule.every().day.at('20:00').do(main)

    while True:
        schedule.run_pending()


if __name__ == '__main__':
    start_time = time.time()
    super_main()
    print(f'\n{time.time() - start_time}')
