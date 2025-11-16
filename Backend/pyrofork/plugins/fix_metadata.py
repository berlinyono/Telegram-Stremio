import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from Backend import db
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.metadata import fetch_tv_metadata, fetch_movie_metadata
from Backend.logger import LOGGER

CANCEL_REQUESTED = False

# -------------------------------
# Progress Bar Helper
# -------------------------------
def progress_bar(done, total, length=20):
    filled = int(length * (done / total)) if total else length
    return f"[{'█' * filled}{'░' * (length - filled)}] {done}/{total}"

# -------------------------------
# ETA Helper
# -------------------------------
def format_eta(seconds):
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {sec}s"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"

# -------------------------------
# CANCEL BUTTON HANDLER
# -------------------------------
@Client.on_callback_query(filters.regex("cancel_fix"))
async def cancel_fix(_, query):
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = True
    await query.message.edit_text("❌ Metadata fixing has been cancelled by the user.")
    await query.answer("Cancelled")

# -------------------------------
# MAIN COMMAND
# -------------------------------
@Client.on_message(filters.command("fixmetadata") & filters.private & CustomFilters.owner, group=10)
async def fix_metadata_handler(_, message):
    global CANCEL_REQUESTED
    CANCEL_REQUESTED = False

    # Count total items
    total_movies = 0
    total_tv = 0

    for i in range(1, db.current_db_index + 1):
        key = f"storage_{i}"
        total_movies += await db.dbs[key]["movie"].count_documents({})
        total_tv += await db.dbs[key]["tv"].count_documents({})

    TOTAL = total_movies + total_tv
    DONE = 0
    start_time = time.time()

    status = await message.reply_text(
        "⏳ Initializing metadata fixing...",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_fix")]
        ])
    )

    CONCURRENCY = 20
    semaphore = asyncio.Semaphore(CONCURRENCY)

    # -------------------------
    # MOVIE UPDATE
    # -------------------------
    async def _safe_update_movie(collection, movie_doc):
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        async with semaphore:
            try:
                imdb_id = movie_doc.get("imdb_id")
                title = movie_doc["title"]
                year = movie_doc.get("release_year")

                # Always fetch metadata to update tmdb_id + imdb_id
                meta = await fetch_movie_metadata(
                    title=title,
                    encoded_string=None,
                    year=year,
                    quality=None,
                    default_id=None
                )

                if meta:
                    update_data = {
                        "tmdb_id": meta.get("tmdb_id"),
                        "imdb_id": meta.get("imdb_id"),
                        "rating": meta.get("rate"),
                    }

                    # only overwrite empty fields (not IDs)
                    for key in ["cast", "description", "genres", "poster", "backdrop", "logo"]:
                        if not movie_doc.get(key):
                            update_data[key] = meta.get(key)

                    await collection.update_one(
                        {"imdb_id": imdb_id},
                        {"$set": update_data}
                    )

                DONE += 1

            except Exception as e:
                LOGGER.exception(f"Error updating movie {movie_doc.get('title')}: {e}")
                DONE += 1

    # -------------------------
    # TV UPDATE
    # -------------------------
    async def _safe_update_tv(collection, tv_doc):
        nonlocal DONE
        if CANCEL_REQUESTED:
            return

        async with semaphore:
            try:
                imdb_id = tv_doc.get("imdb_id")
                title = tv_doc["title"]
                year = tv_doc.get("release_year")

                # Always fetch metadata for refreshed imdb_id + tmdb_id
                meta = await fetch_tv_metadata(
                    title=title,
                    season=1,
                    episode=1,
                    encoded_string=None,
                    year=year,
                    quality=None,
                    default_id=None
                )

                if meta:
                    update_data = {
                        "tmdb_id": meta.get("tmdb_id"),
                        "imdb_id": meta.get("imdb_id"),
                        "rating": meta.get("rate"),
                    }

                    # Only overwrite fields that are empty
                    for key in ["cast", "description", "genres", "poster", "backdrop", "logo"]:
                        if not tv_doc.get(key):
                            update_data[key] = meta.get(key)

                    await collection.update_one(
                        {"imdb_id": imdb_id},
                        {"$set": update_data}
                    )

                # EPISODES UPDATE
                tasks = []
                for season in tv_doc.get("seasons", []):
                    s_num = season.get("season_number")

                    for ep in season.get("episodes", []):
                        e_num = ep.get("episode_number")

                        # Skip if already complete
                        if ep.get("overview") and ep.get("released") and ep.get("episode_backdrop"):
                            continue

                        async def ep_task(s_local=s_num, e_local=e_num):
                            try:
                                ep_meta = await fetch_tv_metadata(
                                    title=title,
                                    season=s_local,
                                    episode=e_local,
                                    encoded_string=None,
                                    year=year,
                                    quality=None,
                                    default_id=None
                                )

                                if ep_meta:
                                    await collection.update_one(
                                        {"imdb_id": imdb_id},
                                        {"$set": {
                                            "seasons.$[s].episodes.$[e].overview": ep_meta.get("episode_overview"),
                                            "seasons.$[s].episodes.$[e].released": ep_meta.get("episode_released"),
                                            "seasons.$[s].episodes.$[e].episode_backdrop": ep_meta.get("episode_backdrop"),
                                        }},
                                        array_filters=[
                                            {"s.season_number": s_local},
                                            {"e.episode_number": e_local}
                                        ]
                                    )
                            except Exception as e:
                                LOGGER.exception(f"Error updating episode {title} S{s_local}E{e_local}: {e}")

                        tasks.append(asyncio.create_task(ep_task()))

                if tasks:
                    for i in range(0, len(tasks), CONCURRENCY):
                        await asyncio.gather(*tasks[i:i+CONCURRENCY], return_exceptions=True)

                DONE += 1

            except Exception as e:
                LOGGER.exception(f"Error updating TV show {tv_doc.get('title')}: {e}")
                DONE += 1

    # -------------------------
    # UPDATE MOVIES
    # -------------------------
    async def update_movies():
        tasks = []
        for i in range(1, db.current_db_index + 1):
            if CANCEL_REQUESTED:
                break

            collection = db.dbs[f"storage_{i}"]["movie"]
            cursor = collection.find({})

            async for movie in cursor:
                if CANCEL_REQUESTED:
                    break

                tasks.append(_safe_update_movie(collection, movie))

                if len(tasks) >= CONCURRENCY * 2:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -------------------------
    # UPDATE TV
    # -------------------------
    async def update_tv():
        tasks = []
        for i in range(1, db.current_db_index + 1):

            collection = db.dbs[f"storage_{i}"]["tv"]
            cursor = collection.find({})

            async for tv in cursor:
                if CANCEL_REQUESTED:
                    break

                tasks.append(_safe_update_tv(collection, tv))

                if len(tasks) >= CONCURRENCY * 2:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -------------------------
    # RUN ALL UPDATES
    # -------------------------
    try:
        await update_movies()
        if not CANCEL_REQUESTED:
            await update_tv()
    except Exception as e:
        LOGGER.exception(f"Error in fix_metadata run: {e}")

    if CANCEL_REQUESTED:
        return

    elapsed = time.time() - start_time
    await status.edit_text(
        f"🎉 **Metadata Fix Completed!**\n"
        f"{progress_bar(DONE, TOTAL)}\n"
        f"⏱ Time Taken: {format_eta(elapsed)}"
    )
