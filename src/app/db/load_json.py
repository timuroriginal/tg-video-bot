import uuid
from datetime import datetime
import json
import asyncio
import asyncpg
from pathlib import Path
from dotenv import load_dotenv
import os

env_path = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(env_path)

DATABASE_URL = os.getenv("DATABASE_URL")
JSON_PATH = Path(__file__).resolve().parents[3] / "videos.json"


def safe_int(val):
    try:
        return int(val or 0)
    except Exception:
        return 0


async def main():
    conn = await asyncpg.connect(DATABASE_URL)

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    videos = payload["videos"]
    print("Videos count:", len(videos))

    # ---------- VIDEOS ----------

    for v in videos:
        video_id = uuid.UUID(v["id"])
        creator_id = uuid.UUID(v["creator_id"])

        video_created_at = datetime.fromisoformat(v["video_created_at"])
        created_at = datetime.fromisoformat(v["created_at"])
        updated_at = datetime.fromisoformat(v["updated_at"])

        await conn.execute(
            """
            INSERT INTO videos (
                id,
                creator_id,
                video_created_at,
                views_count,
                likes_count,
                comments_count,
                reports_count,
                created_at,
                updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO NOTHING
            """,
            video_id,
            creator_id,
            video_created_at,
            safe_int(v.get("views_count")),
            safe_int(v.get("likes_count")),
            safe_int(v.get("comments_count")),
            safe_int(v.get("reports_count")),
            created_at,
            updated_at,
        )

    print("Videos loaded")
    print("First video snapshots:", len(videos[0].get("snapshots", [])))

    # ---------- SNAPSHOTS ----------

    snapshots_loaded = 0

    for v in videos:
        video_id = uuid.UUID(v["id"])

        for s in v.get("snapshots", []):
            snapshot_id = uuid.UUID(s["id"])
            snapshot_at = datetime.fromisoformat(s["created_at"])

            await conn.execute(
                """
                INSERT INTO video_snapshots (
                    id,
                    video_id,
                    views_count,
                    likes_count,
                    comments_count,
                    reports_count,
                    snapshot_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (id) DO NOTHING
                """,
                snapshot_id,
                video_id,
                safe_int(s.get("views_count")),
                safe_int(s.get("likes_count")),
                safe_int(s.get("comments_count")),
                safe_int(s.get("reports_count")),
                snapshot_at,
            )

            snapshots_loaded += 1

            if snapshots_loaded % 500 == 0:
                print("Snapshots inserted:", snapshots_loaded)

    print("Snapshots loaded:", snapshots_loaded)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
