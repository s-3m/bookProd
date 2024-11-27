import asyncio
import os
import pandas as pd
import numpy as np
from typing import Literal


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
                df_dict[file[-6]] = ready_dict
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


async def fetch_request(session, url, headers: dict):
    for _ in range(20):
        async with session.get(url, headers=headers) as resp:
            await asyncio.sleep(4)
            if resp.status == 200:
                return await resp.text()
            else:
                return None
