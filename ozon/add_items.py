import os

from ozon_api import Ozon
import pandas as pd

oz = Ozon(
    client_id=os.getenv("TEST_CLIENT_ID"),
    api_key=os.getenv("TEST_API_KEY"),
    prefix="msk",
    prx=True,
)

# w_id = oz._get_warehouse_id()
# print(w_id)

item_list = pd.read_excel("msk_all.xlsx", keep_default_na=False).to_dict(
    orient="records"
)

success_items = oz.add_items(item_list)
# oz.update_stock(success_items, update_price=False)
