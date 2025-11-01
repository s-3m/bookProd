import asyncio
import json
import os
import re
import random
import time
from pathlib import Path

import pandas as pd
import polars as pl
from typing import Literal
import aiohttp
import requests
from loguru import logger
from ozon.ozon_api import get_items_list


def filesdata_to_dict(
    folder_path: str, combined=False, return_df=False
) -> dict | pd.DataFrame | None:
    frame_list = []
    if combined:
        for dirName, subdirList, fileList in os.walk(folder_path):
            for file in fileList:
                match = re.search(r"seller-(\d+)-time", file)
                seller_id = match.group(1)
                df = pd.read_csv(
                    f"{dirName}/{file}", sep=";", converters={"Артикул": str}
                )[["Артикул"]]
                df["on sale"]: str = ""
                df["seller_id"] = seller_id
                frame_list.append(df)
        try:
            result_frame = (
                pd.concat(frame_list).replace({"'": ""}, regex=True).drop_duplicates()
            )
            if return_df:
                return result_frame
            else:
                result_frame = result_frame.drop_duplicates(subset="Артикул")
                result_dict = result_frame.set_index("Артикул").to_dict(orient="index")
                for i in result_dict:
                    result_dict[i]["article"] = i
                return result_dict
        except ValueError as e:
            print(e)
            return None

    else:
        df_dict = {}
        for dirName, subdirList, fileList in os.walk(folder_path):
            for file in fileList:
                template = r"\d+"
                price_number = re.search(template, file).group()

                df: pd.DataFrame = pl.read_excel(
                    f"{dirName}/{file}", sheet_id=2, read_options={"header_row": 2}
                ).to_pandas()
                df = df.astype({"Артикул": str})[["Артикул"]].drop_duplicates()
                df["price"] = ""
                # df = pd.read_excel(
                #     f"{dirName}/{file}",
                #     converters={"Артикул": str},
                #     sheet_name=1,
                #     header=2,
                # )[["Артикул"]].drop_duplicates()
                # df["price"] = ""

                df = df.where(df.notnull(), None)
                ready_dict = df.set_index("Артикул").to_dict(orient="index")
                if None in ready_dict:
                    del ready_dict[None]
                elif "None" in ready_dict:
                    del ready_dict["None"]
                df_dict[price_number] = ready_dict
        return df_dict


def clear_caps_text(text):
    sentense = text.split(".")
    capitalized_text = ". ".join([i.strip().capitalize() for i in sentense])
    splitting_caps_text = capitalized_text.split()

    need_index = []
    for item in splitting_caps_text:
        if len(item) == 2 and "." in item:
            need_index.append(splitting_caps_text.index(item))
    if need_index:
        for index in need_index:
            splitting_caps_text[index] = splitting_caps_text[index].capitalize()
    return " ".join(splitting_caps_text)


DF_danger_string = pd.read_excel(
    f"{os.path.abspath(os.path.dirname(__file__))}/danger_string.xlsx"
)
in_title = DF_danger_string[DF_danger_string.columns[0]].dropna().to_list()
in_description = DF_danger_string[DF_danger_string.columns[1]].dropna().to_list()
hard_delete = DF_danger_string[DF_danger_string.columns[2]].dropna().to_list()


async def check_danger_string(
    base_string: str, place_to_check: Literal["title", "description"]
):
    if place_to_check == "title":
        if any(x.lower() in base_string.lower() for x in hard_delete):
            return None
        if any(x.lower() in base_string.lower() for x in in_title):
            for i in in_title:
                if i in base_string:
                    base_string = base_string.replace(i, "")
        base_string = clear_caps_text(base_string)
    elif place_to_check == "description":
        if any(x.lower() in base_string.lower() for x in in_description):
            for i in in_description:
                if i in base_string:
                    base_string = base_string.replace(i, "")

    return base_string


def check_wrong_chars(base_string):
    cleaned_chars = []
    for char in base_string:
        if (
            char == "\t" or char == "\n" or char == "\r"
        ):  # разрешаем табуляцию, перевод строки
            cleaned_chars.append(char)
        elif char.isprintable():  # разрешаем печатные символы
            cleaned_chars.append(char)
        # Все остальные символы игнорируются

    text = "".join(cleaned_chars)
    return text


with open(Path(__file__).parent / "proxy.txt") as f:
    PROXIES = f.readlines()


def sync_fetch_request(url, headers, cookies=None, use_proxy=False):
    response_status_code = None
    selected_proxy = None
    if use_proxy:
        selected_proxy = random.choice(PROXIES).strip()
        proxy = {"http": f"{selected_proxy}", "https": f"{selected_proxy}"}
    else:
        proxy = None
    for _ in range(10):
        try:
            response = requests.get(
                url, headers=headers, cookies=cookies, timeout=30, proxies=proxy
            )
            time.sleep(1)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                return "404"
            else:
                response_status_code = response.status_code
        except Exception as e:
            logger.exception(f"ERROR - {e} | proxy - {selected_proxy}")
            return "proxy error"
    return response_status_code


async def fetch_request(session, url, headers: dict, sleep=4, proxy=None):
    for _ in range(20):
        try:
            async with session.get(url, headers=headers, proxy=proxy) as resp:
                await asyncio.sleep(sleep) if sleep else None
                if resp.status == 200:
                    return await resp.text()
                elif resp.status == 404:
                    return "404"
                elif resp.status == 503:
                    return "503"
        except TimeoutError as e:
            logger.exception(e)
            continue
        except aiohttp.client_exceptions.ClientConnectorError as e:
            logger.exception(e)
            continue
        except aiohttp.client_exceptions.ServerDisconnectedError as e:
            logger.exception(e)
            continue
        except Exception as e:
            logger.exception(e)
    return None


def check_archived_books(df_for_add: pd.DataFrame) -> pd.DataFrame:
    df_archive = pd.read_excel(Path(__file__).parent / "arch_for_check.xlsx")
    try:
        df_result = df_for_add[~df_for_add["ISBN"].isin(df_archive["ISBN"])]
        return df_result
    except KeyError:
        df_result = df_for_add[~df_for_add["ISBN:"].isin(df_archive["ISBN"])]
        return df_result


def clean_excel_text(text):
    """Безопасная очистка текста для Excel"""
    if not isinstance(text, str):
        return text

    # Удаляем управляющие символы (кроме табуляции, новой строки и возврата каретки)
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    # Заменяем проблемные кавычки на обычные (опционально)
    text = text.replace("«", '"').replace("»", '"').replace("\n\n", "\n")
    return text


def write_result_files(
    base_dir: str,
    prefix: str,
    all_books_result,
    id_to_add: list | tuple[pd.DataFrame, pd.DataFrame],
    replace_photo: bool = False,
):
    all_result_df = pd.DataFrame(all_books_result).drop_duplicates(subset="Артикул_OZ")
    clear_all_result_df = all_result_df.map(clean_excel_text)
    clear_all_result_df.to_excel(
        f"{base_dir}/result/{prefix}_all.xlsx", index=False, engine="openpyxl"
    )

    if isinstance(id_to_add, list):
        df_add = pd.DataFrame(id_to_add).drop_duplicates(subset="Артикул_OZ")
        clean_df_add = df_add.map(clean_excel_text)
        clean_df_add.sort_values("Наличие").drop_duplicates(
            subset="Название", keep="last"
        ).sort_values("Артикул_OZ")
        # Check "add books" not in archive books
        clean_df_add = check_archived_books(df_for_add=clean_df_add)

        if replace_photo:
            del clean_df_add["Фото_y"]
            clean_df_add.rename(columns={"Фото_x": "Фото"}, inplace=True)

        clean_df_add.to_excel(
            f"{base_dir}/result/{prefix}_add.xlsx", index=False, engine="openpyxl"
        )

    elif isinstance(id_to_add, tuple):
        new_shop_df = id_to_add[0]
        old_shop_df = id_to_add[1]
        new_shop_df.drop_duplicates(subset="Название", keep="last", inplace=True)
        old_shop_df.drop_duplicates(subset="Название", keep="last", inplace=True)

        # Check "add books" not in archive books
        new_shop_add = check_archived_books(df_for_add=new_shop_df)
        old_shop_add = check_archived_books(df_for_add=old_shop_df)

        new_shop_add.to_excel(
            f"{base_dir}/result/{prefix}_add_new.xlsx", index=False, engine="openpyxl"
        )
        old_shop_add.to_excel(
            f"{base_dir}/result/{prefix}_add_old.xlsx", index=False, engine="openpyxl"
        )


def exclude_else_shops_books(items_on_add: list[dict], exclude_shop: str | None = None):
    add_df = pl.DataFrame(items_on_add, infer_schema_length=100000)

    mg_path = "/media/source/mg/result/mg_all.xlsx"
    msk_path = "/media/source/msk/result/msk_all.xlsx"
    mdk_path = "/media/source/mdk/result/mdk_all.xlsx"
    chit_path = "/media/source/chitai/result/chit_gor_all.xlsx"

    # mg_path = "mg/source/result/mg_all.xlsx"
    # msk_path = "msk/source/result/msk_all.xlsx"
    # mdk_path = "mdk/source/result/mdk_all.xlsx"
    # chit_path = "chitai/source/result/chit_gor_all.xlsx"

    all_shops = {
        "mg": mg_path,
        "msk": msk_path,
        "mdk": mdk_path,
        "chit": chit_path,
    }
    if exclude_shop:
        del all_shops[exclude_shop]

    for shop in all_shops:
        pl_df = pl.read_excel(all_shops[shop])
        add_df = add_df.join(pl_df, on="ISBN", how="anti")

    result = add_df.to_dicts()
    return result


def forming_add_files(
    result_df: pd.DataFrame, prefix: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    polars_df = pl.from_pandas(result_df)
    items_list_new_shop = get_items_list(
        prefix=prefix, visibility="ALL", shop_category="new"
    )
    archived_items_list_new_shop = get_items_list(
        prefix=prefix, visibility="ARCHIVED", shop_category="new"
    )
    items_list_old_shop = get_items_list(
        prefix=prefix, visibility="ALL", shop_category="old"
    )
    # archived_items_list_old_shop = get_items_list(
    #     prefix=prefix, visibility="ARCHIVED", shop_category="old")

    archived_items_list_old_shop = (
        []
    )  # Поменяно на время, после нужно удалить это и раскоментировать строки выше
    items_list_new_shop.extend(archived_items_list_new_shop)
    items_list_old_shop.extend(archived_items_list_old_shop)

    df_items_list_new_shop = pl.DataFrame(items_list_new_shop)[["Артикул"]].rename(
        {"Артикул": "Артикул_OZ"}
    )
    df_items_list_old_shop = pl.DataFrame(items_list_old_shop)[["Артикул"]].rename(
        {"Артикул": "Артикул_OZ"}
    )

    result_new_shop = polars_df.join(
        df_items_list_new_shop, on="Артикул_OZ", how="anti"
    ).to_pandas()

    result_old_shop = polars_df.join(
        df_items_list_old_shop, on="Артикул_OZ", how="anti"
    ).to_pandas()
    return result_new_shop, result_old_shop


def give_me_sample(
    base_dir: str,
    prefix: str,
    without_merge=False,
    merge_obj="Ссылка",
    ozon_in_sale=None,
) -> list[dict]:
    path_to_sample = os.path.join(base_dir, "..")
    if not ozon_in_sale:
        df1 = filesdata_to_dict(f"{path_to_sample}/sale", combined=True, return_df=True)
    else:
        df1 = pd.DataFrame(ozon_in_sale)

    merge_obj_translate = "link" if merge_obj == "Ссылка" else merge_obj

    if df1 is not None:
        if not without_merge:
            df2 = pd.read_excel(
                f"{path_to_sample}/result/{prefix}_all.xlsx",
                converters={"Артикул_OZ": str, merge_obj: str},
            )[["Артикул_OZ", merge_obj]]
            df2.columns = ["Артикул", merge_obj]

            sample = pd.merge(
                df1[["Артикул", "seller_id"]], df2, on="Артикул", how="left"
            )
            sample.columns = ["article", "seller_id", merge_obj_translate]
            sample["price"] = None
        else:
            sample = df1[["Артикул", "seller_id"]]
            sample.columns = ["article", "seller_id"]
            sample["price"] = None
        sample = sample.drop_duplicates()
        # sale_files = os.listdir(f"{path_to_sample}/sale")
        # for i in sale_files:
        #     os.remove(f"{path_to_sample}/sale/{i}")
    else:
        sample = pd.read_excel(
            f"{base_dir}/{prefix}_new_stock.xlsx",
            converters={"article": str, "link": str, merge_obj: str, "seller_id": str},
        )
    sample["stock"] = ""
    sample = sample.where(sample.notnull(), None)
    sample = sample.to_dict("records")
    return sample


def quantity_checker(sample: list[dict]) -> bool:
    zero_count = 0
    for i in sample:
        if i["stock"] in (0, "0"):
            zero_count += 1

    percent = (len(sample) * 50) / 100
    if zero_count > percent:
        return False
    else:
        return True
