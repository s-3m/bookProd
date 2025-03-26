import asyncio
from aiogram import Bot
from aiogram.types.input_file import FSInputFile
import os
from aiogram.types.input_media_document import InputMediaDocument
from loguru import logger


@logger.catch
async def tg_send_files(files: list[str], subject):
    bot = Bot(os.getenv("BOT_TOKEN"))
    files_list = []
    for file in files:
        file = FSInputFile(file)
        file = InputMediaDocument(media=file, caption=subject)
        files_list.append(file)
    await bot.send_media_group(os.getenv("CHAT_ID"), files_list)
    await bot.session.close()
    logger.success(f"Files ({subject}) was send successfully")


@logger.catch
async def tg_send_msg(prefix):
    bot = Bot(os.getenv("BOT_TOKEN"))
    for reciver in (os.getenv("CHAT_ID"), os.getenv("CHAT_ID_TEST")):
        await bot.send_message(
            reciver,
            f"После парсинга {prefix} обнаружено слишком много позиций с остатком 0."
            f" Данные автоматически не загружены на OZON. Необходимо проверить данные вручную!",
        )

    await bot.session.close()


if __name__ == "__main__":
    a = []
    for dirpath, _, filenames in os.walk("mg/compare"):
        for f in filenames:
            a.append(os.path.abspath(os.path.join(dirpath, f)))

    asyncio.run(tg_send_files(a, "test"))
