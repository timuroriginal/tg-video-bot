import asyncio
import asyncpg
from pathlib import Path
from dotenv import load_dotenv
import os

env_path = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(env_path)

DATABASE_URL = os.getenv("DATABASE_URL")


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    value = await conn.fetchval("SELECT 1;")
    print("DB OK:", value)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

