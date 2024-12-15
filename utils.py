import asyncio
import os
import re
import pandas as pd
import numpy as np
from typing import Literal
import aiohttp


def filesdata_to_dict(folder_path: str, combined=False, return_df=False) -> dict | None:
    frame_list = []
    if combined:
        for dirName, subdirList, fileList in os.walk(folder_path):
            for file in fileList:
                df = pd.read_csv(
                    f"{dirName}/{file}", sep=";", converters={"Артикул": str}
                )[["Артикул"]]
                df["on sale"]: str = ""
                frame_list.append(df)
        try:
            result_frame = (
                pd.concat(frame_list).replace({"'": ""}, regex=True).drop_duplicates()
            )
            return (
                result_frame
                if return_df
                else result_frame.set_index("Артикул").to_dict(orient="index")
            )
        except ValueError:
            return None

    else:
        df_dict = {}
        for dirName, subdirList, fileList in os.walk(folder_path):
            for file in fileList:
                template = r"\d+"
                price_number = re.search(template, file).group()
                df = pd.read_excel(
                    f"{dirName}/{file}",
                    converters={"Артикул": str},
                    sheet_name=1,
                    header=2,
                )[["Артикул"]].drop_duplicates()
                df["price"] = ""

                df = df.where(df.notnull(), None)
                ready_dict = df.set_index("Артикул").to_dict(orient="index")
                if None in ready_dict:
                    del ready_dict[None]
                df_dict[price_number] = ready_dict
        return df_dict


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
        if any(x in base_string for x in hard_delete):
            return None
        if any(x in base_string for x in in_title):
            for i in in_title:
                if i in base_string:
                    base_string = base_string.replace(i, "")
    elif place_to_check == "description":
        if any(x in base_string for x in in_description):
            for i in in_description:
                if i in base_string:
                    base_string = base_string.replace(i, "")

    return base_string


# proxy = "http://4XRUpQ:cKCEtZ@46.161.45.111:9374"


async def fetch_request(session, url, headers: dict, sleep=4, proxy=None):
    for _ in range(20):
        try:
            async with session.get(url, headers=headers, proxy=proxy) as resp:
                await asyncio.sleep(sleep) if sleep else None
                if resp.status == 200:
                    return await resp.text()
                elif resp.status == 404:
                    return "404"
        except TimeoutError:
            continue
        except aiohttp.client_exceptions.ClientConnectorError:
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
):
    all_result_df = pd.DataFrame(all_books_result).drop_duplicates(subset="Артикул")
    all_result_df.to_excel(f"{base_dir}/result/{prefix}_all.xlsx", index=False)

    df_add = pd.DataFrame(id_to_add).drop_duplicates(subset="Артикул")
    df_add.to_excel(f"{base_dir}/result/{prefix}_add.xlsx", index=False)

    df_del = pd.DataFrame(id_to_del).drop_duplicates()
    df_del.columns = ["Артикул"]
    df_del.to_excel(f"{base_dir}/result/{prefix}_del.xlsx", index=False)

    df_not_in_sale = pd.DataFrame().from_dict(not_in_sale, orient="index")
    df_not_in_sale.index.name = "article"
    df_not_in_sale.to_excel(f"{base_dir}/result/{prefix}_not_in_sale.xlsx")

    for price_item in prices:
        df_result = pd.DataFrame().from_dict(prices[price_item], orient="index")
        df_result.index.name = "article"
        df_result.to_excel(
            f"{base_dir}/result/{prefix}_price_{price_item}.xlsx", index=True
        )

    df_not_in_sale2 = pd.DataFrame().from_dict(not_in_sale, orient="index")
    df_not_in_sale2.index.name = "article"
    df_not_in_sale2 = df_not_in_sale2.loc[df_not_in_sale2["on sale"] == "да"][
        ["article"]
    ]
    df_not_in_sale2.to_excel(f"{base_dir}/result/{prefix}_not_in_sale2.xlsx")


def give_me_sample(
    base_dir: str, prefix: str, without_merge=False, merge_obj="Ссылка"
) -> list[dict]:
    path_to_sample = os.path.join(base_dir, "..")
    df1 = filesdata_to_dict(f"{path_to_sample}/sale", combined=True, return_df=True)
    merge_obj_translate = "link" if merge_obj == "Ссылка" else merge_obj

    if df1 is not None:
        if not without_merge:
            df2 = pd.read_excel(
                f"{path_to_sample}/result/{prefix}_all.xlsx",
                converters={"Артикул": str, merge_obj: str},
            )[["Артикул", merge_obj]]

            sample = pd.merge(df1[["Артикул"]], df2, on="Артикул", how="left")
            sample.columns = ["article", merge_obj_translate]
        else:
            sample = df1[["Артикул"]]
            sample.columns = ["article"]
        sale_files = os.listdir(f"{path_to_sample}/sale")
        for i in sale_files:
            os.remove(f"{path_to_sample}/sale/{i}")
    else:
        sample = pd.read_excel(
            f"{base_dir}/{prefix}_new_stock.xlsx",
            converters={"article": str, "link": str, merge_obj: str},
        )
    sample["stock"] = ""
    sample = sample.where(sample.notnull(), None)
    sample = sample.to_dict("records")
    return sample
