import os

from ozon_api import Ozon
import pandas as pd

oz = Ozon(
    client_id=os.getenv("TEST_CLIENT_ID"), api_key=os.getenv("TEST_API_KEY"), prefix="msk"
)

item_list = pd.read_excel("msk_all.xlsx", keep_default_na=False).to_dict(
    orient="records"
)

oz.add_items(item_list)
