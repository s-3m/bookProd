import os
import pandas as pd
import numpy as np


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
