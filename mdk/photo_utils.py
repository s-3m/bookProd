import asyncio
import os

import requests
from loguru import logger
import aiohttp
from PIL import Image, ImageFilter
from io import BytesIO
import pandas as pd
import yadisk
from s3_utils import s3_client

BASE_API_URL = "https://cloud-api.yandex.net"


async def save_to_ya(img_path, item):
    async with yadisk.AsyncClient(
        token="y0_AgAAAAAAyABTAAzxJQAAAAEb9rrZAADaymYjvotLfbYhScJtd11u0UowZQ",
        session="aiohttp",
    ) as ya_client:
        try:
            res = await ya_client.upload(
                img_path,
                f"/MDK_photo/{str(item["Артикул_OZ"])[:-2]}.png",
                overwrite=True,
                timeout=999,
            )
            await asyncio.sleep(4)
            pub_file = await ya_client.publish(
                res.path,
                fields="public_url",
                timeout=90,
            )
            await asyncio.sleep(4)
            file_meta = await pub_file.get_meta()
            print(file_meta["public_url"])
            try:
                public_url = file_meta["public_url"]
            except KeyError:
                public_url = None

            return public_url
        except yadisk.exceptions.YaDiskConnectionError as e:
            logger.exception(e)
            return "not"
        except Exception as e:
            logger.exception(e)


async def crop_image(image, img_name):
    path_photo = f"mdk_{img_name}.png"
    try:
        image = Image.open(BytesIO(image))
        im_crop = image.crop((0, 0, image.width, image.height - 20))
        scale_ = 2  # 300%
        new_size = (int(im_crop.size[0] * scale_), int(im_crop.size[1] * scale_))
        new_img = im_crop.resize(new_size)
        img_filter = new_img.filter(ImageFilter.SHARPEN)
        img_byte_arr = BytesIO()
        img_filter.save(img_byte_arr, format="PNG")
        img_byte_arr.seek(0)
        return img_byte_arr

    except ValueError:
        image.save(path_photo, "PNG")
        return os.path.abspath(path_photo)


count_replace_photo = 1

sem = asyncio.Semaphore(3)


async def photo_processing(item):
    global count_replace_photo
    try:
        for _ in range(5):
            await asyncio.sleep(0.5)
            try:
                resp = requests.get(item["Фото_x"], timeout=30)
                resp = resp.content
                if not resp:
                    print(f"not resp {item["Фото_x"]}")
                img_path = await crop_image(resp, item["Фото_x"].split("/")[-1][:-4])
                new_url = await s3_client.upload_file(
                    file=img_path,
                    name=f"mdk_{item["Фото_x"].split("/")[-1][:-4]}.png",
                )
                item["Фото_x"] = new_url
                print(f"\rReplace photo done - {count_replace_photo}", end="")
                count_replace_photo += 1
                break
            except Exception as e:
                logger.exception(e)
                continue

    except Exception as e:
        logger.exception(e)
        item["Фото_x"] = "https://zapobedu21.ru/images/26.07.2017/kniga.jpg"


async def replace_photo(add_list: list[dict]) -> pd.DataFrame:
    print()
    logger.info("Start replace photo")

    path_to_chit = "/media/source/chitai/result/chit_gor_all.xlsx"
    # path_to_chit = "../chitai/source/result/chit_gor_all.xlsx"
    chit_gor_df = pd.read_excel(path_to_chit)[["ISBN", "Фото"]]
    chit_gor_df = chit_gor_df.where(chit_gor_df.notnull(), None)

    add_df = pd.DataFrame(add_list)

    merge_result = pd.merge(
        add_df,
        chit_gor_df,
        on="ISBN",
        how="left",
    )
    merge_result = merge_result.where(merge_result.notnull(), None)

    result = merge_result.to_dict("records")

    for i in result:
        if i["Фото_y"]:
            i["Фото_x"] = i["Фото_y"]
        else:
            if i["Фото_x"] is not None:
                await photo_processing(i)

    result_df = pd.DataFrame(result)
    del result_df["Фото_y"]
    result_df.rename(columns={"Фото_x": "Фото"}, inplace=True)
    result_df.drop_duplicates(subset="Артикул_OZ", inplace=True)
    return result_df


if __name__ == "__main__":
    sample = pd.read_excel("mdk_all.xlsx", keep_default_na=False)
    sample = sample.where(sample.notnull(), None).to_dict(orient="records")
    asyncio.run(replace_photo(sample))
