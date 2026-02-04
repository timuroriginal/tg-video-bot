import asyncio
import os
import re
from dotenv import load_dotenv
import asyncpg

from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import CommandStart

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

db_pool = None


@dp.message(CommandStart())
async def start(message: Message):
    await message.answer(
        "Бот жив.\n"
        "Примеры:\n"
        "— сколько всего видео\n"
        "— сколько видео больше 1000\n"
        "— прирост просмотров за день"
    )


@dp.message()
async def handle(message: Message):
    text = message.text.lower()

    m = re.search(r"(\d+)", text)

    # прирост просмотров за день
    if "прирост" in text and "просмотр" in text:
        async with db_pool.acquire() as conn:
            growth = await conn.fetchval(
                """
                SELECT COALESCE(SUM(s2.views_count - s1.views_count), 0)
                FROM video_snapshots s1
                JOIN video_snapshots s2
                  ON s1.video_id = s2.video_id
                WHERE s2.snapshot_at::date = CURRENT_DATE
                  AND s1.snapshot_at::date = CURRENT_DATE - INTERVAL '1 day';
                """
            )

        await message.answer(f"Прирост просмотров за день: {growth}")
        return

    # сколько видео больше N
    if "сколько" in text and "видео" in text and m:
        threshold = int(m.group(1))

        async with db_pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM videos WHERE views_count > $1;",
                threshold
            )

        await message.answer(f"Видео с просмотрами больше {threshold}: {count}")
        return

    # сколько всего видео
    if "сколько" in text and "видео" in text:
        async with db_pool.acquire() as conn:
            count = await conn.fetchval("SELECT count(*) FROM videos;")

        await message.answer(f"Всего видео: {count}")
        return

    await message.answer("Не понял запрос.")


async def main():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

