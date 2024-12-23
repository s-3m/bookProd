import asyncio
import os
from datetime import datetime
from loguru import logger
import aiohttp
from PIL import Image
from io import BytesIO
import pandas as pd

BASE_API_URL = "https://cloud-api.yandex.net"


async def save_to_ya(session, img_path):
    headers = {
        "Authorization": "y0_AgAAAAAAyABTAAzxJQAAAAEb9rrZAADaymYjvotLfbYhScJtd11u0UowZQ"
    }
    # Получение ссылки для закгрузки файла
    api_url = f"{BASE_API_URL}/v1/disk/resources/upload"
    path_in_ya = f"/MDK_photo/mdk_{datetime.timestamp(datetime.now())}.jpeg"
    params = {"path": path_in_ya}

    async with session.get(api_url, params=params, headers=headers) as res:
        res = await res.json()
        await asyncio.sleep(2)

    # Открытие файла и загрузка его по полученной ссылке
    with open(img_path, "rb") as f:
        try:
            async with session.put(res["href"], data=f) as response:
                ...
        except KeyError:
            pass

    # Открытие доступа к файлу по ссылке
    publish_url = f"{BASE_API_URL}/v1/disk/resources/publish"
    publish_params = {
        "path": path_in_ya,
        "fields": "public_url",
    }
    async with session.put(
        publish_url, params=publish_params, headers=headers
    ) as resp_pub:
        resp_pub = await resp_pub.json(content_type=None)
    await asyncio.sleep(2)

    # Получение общей ссылки на файл
    metadata_url = f"{BASE_API_URL}/v1/disk/resources"
    metadata_params = {
        "path": path_in_ya,
        "fields": "public_url",
    }
    async with session.get(
        metadata_url, params=metadata_params, headers=headers
    ) as response:
        await asyncio.sleep(2)
        try:
            ya_photo_url = await response.json()
            ya_photo_url = ya_photo_url["public_url"]
        except KeyError:
            pass
    return ya_photo_url


async def crop_image(image):
    image = Image.open(BytesIO(image))
    im_crop = image.crop((0, 0, image.width, image.height - 30))
    timestamp_for_name = datetime.timestamp(datetime.now())
    path_photo = f"mdk_{timestamp_for_name}.jpeg"
    im_crop.save(path_photo, "JPEG")
    full_path = os.path.abspath(path_photo)
    return full_path


count_replace_photo = 1


async def photo_processing(session, item):
    global count_replace_photo
    async with session.get(item["Фото_x"]) as a:
        await asyncio.sleep(3)
        a = await a.content.read()
        img_path = await crop_image(a)
    new_url = await save_to_ya(session, img_path)
    os.remove(img_path)
    item["Фото_x"] = new_url
    print(f"\rReplace photo done - {count_replace_photo}", end="")
    count_replace_photo += 1


@logger.catch
async def replace_photo(base_dir: str, add_list: list[dict]):
    print("Start process")
    path_to_chit = os.path.join(
        base_dir, "..", "chitai/source/result/chit-gor_all.xlsx"
    )
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
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit_per_host=4), trust_env=True
    ) as session:
        tasks = []
        for i in result:
            if i["Фото_y"]:
                i["Фото_x"] = i["Фото_y"]
            else:
                task = asyncio.create_task(photo_processing(session, i))
                tasks.append(task)
        await asyncio.gather(*tasks)
    return result
