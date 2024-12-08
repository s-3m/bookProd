from mg.compare import main as mg_main
from msk.compare import main as msk_main
from globus.compare import main as globus_main
import schedule
from dotenv import load_dotenv
from loguru import logger


def main():
    load_dotenv("./.env")
    # try:
    #     globus_main()
    # except Exception as e:
    #     logger.exception(f"Exception in globus")
    try:
        mg_main()
    except Exception as e:
        logger.exception(f"Exception in mg")
    try:
        msk_main()
    except Exception as e:
        logger.exception(f"Exception in msk")


def super_main():
    schedule.every().day.at("21:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
