import os
import time
import requests
from PIL import Image
from io import BytesIO

BASE_API_URL = "https://cloud-api.yandex.net"


def crop_image(image):
    image = Image.open(BytesIO(image))
    im_crop = image.crop((0, 0, image.width, image.height - 300))
    path_photo = "cropped.png"
    im_crop.save(path_photo, "PNG")
    full_path = os.path.abspath(path_photo)
    return full_path


def photo_processing():
    a = requests.get(
        "https://i.pinimg.com/originals/ce/af/62/ceaf62dcca5c04ae5831d9351088aeaf.png"
    )
    time.sleep(1)
    img = crop_image(a.content)
    print(img)
    headers = {
        "Authorization": "y0_AgAAAAAAyABTAAzxJQAAAAEb9rrZAADaymYjvotLfbYhScJtd11u0UowZQ"
    }
    api_url = f"{BASE_API_URL}/v1/disk/resources/upload"
    path_in_ya = f"/MDK_photo/ddddd.jpg"
    params = {"path": path_in_ya, "fields": "public_url", "overwrite": True}

    res = requests.get(api_url, params=params, headers=headers).json()
    time.sleep(0.5)
    with open(img, "rb") as f:
        try:
            requests.put(res["href"], files={"file": f})
        except KeyError:
            print(res)

    publish_url = f"{BASE_API_URL}/v1/disk/resources/publish"
    publish_params = {
        "path": path_in_ya,
        "fields": "public_url",
    }
    requests.put(publish_url, params=publish_params, headers=headers)
    time.sleep(0.5)

    metadata_url = f"{BASE_API_URL}/v1/disk/resources"
    metadata_params = {
        "path": path_in_ya,
        "fields": "public_url",
    }
    response = requests.get(metadata_url, params=metadata_params, headers=headers)
    ya_photo_url = response.json()["public_url"]
    return ya_photo_url


print(photo_processing())
