import asyncio
from aiogram import Bot
from aiogram.types.input_file import FSInputFile
import os
from loguru import logger
from aiogram.types.input_media_document import InputMediaDocument


@logger.catch
async def tg_send_files(files:list[str], subject):
    logger.info(f"Start bot initialization ({subject})")
    bot = Bot(os.getenv('BOT_TOKEN'))
    files_list = []
    logger.info(f"Attaching files ({subject})")
    for file in files:
        file = FSInputFile(file)
        file = InputMediaDocument(media=file, caption=subject)
        files_list.append(file)
    logger.info(f"Sending files ({subject})")
    await bot.send_media_group(os.getenv('CHAT_ID'), files_list)
    await bot.session.close()
    logger.success(f"Sending was finished successfully ({subject})")


if __name__ == '__main__':
    a = []
    for dirpath, _, filenames in os.walk('mg/compare'):
        for f in filenames:
            a.append(os.path.abspath(os.path.join(dirpath, f)))

    asyncio.run(tg_send_files(a, 'test'))
