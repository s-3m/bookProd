import asyncio
import os
import pandas as pd
import numpy as np
from typing import Literal


def filesdata_to_dict(folder_path: str, combined=False) -> dict | list[dict]:
    frame_list = []
    if combined:
        for dirName, subdirList, fileList in os.walk(folder_path):
            for file in fileList:
                df = pd.read_csv(
                    f"{dirName}/{file}", sep=";", converters={"article": str}
                )[["Артикул"]]
                df["on sale"]: str = np.nan
                frame_list.append(df)
        result_frame = (
            pd.concat(frame_list).replace({"'": ""}, regex=True).drop_duplicates()
        )
        return result_frame.set_index("Артикул").to_dict(orient="index")
    else:
        for dirName, subdirList, fileList in os.walk(folder_path):
            for file in fileList:
                df = pd.read_excel(
                    f"{dirName}/{file}",
                    converters={"article": str},
                    sheet_name=1,
                    header=2,
                )[["Артикул"]].drop_duplicates()
                df["price"] = np.nan
                df = df.where(df.notnull(), None)
                ready_dict = df.set_index("Артикул").to_dict(orient="index")
                if None in ready_dict:
                    del ready_dict[None]
                frame_list.append(ready_dict)
        return frame_list


DF_danger_string = pd.read_excel("danger_string.xlsx")
in_title = DF_danger_string[DF_danger_string.columns[0]].dropna().to_list()
in_description = DF_danger_string[DF_danger_string.columns[1]].dropna().to_list()
hard_delete = DF_danger_string[DF_danger_string.columns[2]].dropna().to_list()


async def check_danger_string(base_string: str, place_to_check: Literal["title", "description"]):
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
