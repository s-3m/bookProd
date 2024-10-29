from mg.compare import main as mg_main
from msk.compare import main as msk_main
import schedule
from dotenv import load_dotenv


def main():
    mg_main()
    msk_main()


def super_main():
    load_dotenv()
    schedule.every().day.at("03:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
