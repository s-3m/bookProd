import asyncio
import json
import os
import re
import random
import time

import pandas as pd
import polars as pl
from typing import Literal
import aiohttp
import requests


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


def sync_fetch_request(url, headers, cookies=None):
    response_status_code = None
    for _ in range(10):
        response = requests.get(url, headers=headers, cookies=cookies, timeout=30)
        time.sleep(2)
        if response.status_code == 200:
            return response.text
        elif response.status_code == 404:
            return "404"
        else:
            response_status_code = response.status_code
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
        except TimeoutError:
            continue
        except aiohttp.client_exceptions.ClientConnectorError:
            continue
        except aiohttp.client_exceptions.ServerDisconnectedError:
            continue
    return None


def write_result_files(
    base_dir: str,
    prefix: str,
    all_books_result,
    id_to_add: list,
    id_to_del: list | set,
    not_in_sale: dict,
    prices: dict[str, dict],
    replace_photo: bool = False,
):
    all_result_df = pd.DataFrame(all_books_result).drop_duplicates(subset="Артикул_OZ")
    all_result_df.to_excel(f"{base_dir}/result/{prefix}_all.xlsx", index=False)

    df_add = pd.DataFrame(id_to_add).drop_duplicates(subset="Артикул_OZ")
    df_add = (
        df_add.sort_values("Наличие")
        .drop_duplicates(subset="Название", keep="last")
        .sort_values("Артикул_OZ")
    )
    if replace_photo:
        del df_add["Фото_y"]
        df_add.rename(columns={"Фото_x": "Фото"}, inplace=True)
    df_add.to_excel(f"{base_dir}/result/{prefix}_add.xlsx", index=False)

    df_del = pd.DataFrame(id_to_del).drop_duplicates()
    df_del.columns = ["Артикул"]
    df_del.to_excel(f"{base_dir}/result/{prefix}_del.xlsx", index=False)

    df_not_in_sale = pd.DataFrame().from_dict(not_in_sale, orient="index")
    df_not_in_sale = df_not_in_sale.loc[df_not_in_sale["on sale"] == "да"][["article"]]
    df_not_in_sale.to_excel(f"{base_dir}/result/{prefix}_not_in_sale.xlsx", index=False)

    for price_item in prices:
        df_result = pd.DataFrame().from_dict(prices[price_item], orient="index")
        df_result.index.name = "article"
        df_result.to_excel(
            f"{base_dir}/result/{prefix}_price_{price_item}.xlsx", index=True
        )


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
