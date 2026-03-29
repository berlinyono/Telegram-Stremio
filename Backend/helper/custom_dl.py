import asyncio
from pyrogram import utils, raw
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Session, Auth
from typing import Dict, Union
from Backend.logger import LOGGER
from Backend.helper.exceptions import FIleNotFound
from Backend.helper.pyro import get_file_ids
from Backend.pyrofork.bot import work_loads
from pyrogram import Client, utils, raw


class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.__cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, chat_id: int, message_id: int) -> FileId:
        if message_id not in self.__cached_file_ids:
            file_id = await get_file_ids(self.client, int(chat_id), int(message_id))
            if not file_id:
                LOGGER.info('Message with ID %s not found!', message_id)
                raise FIleNotFound
            self.__cached_file_ids[message_id] = file_id
        return self.__cached_file_ids[message_id]

    async def yield_file(
        self, 
        file_id: FileId, 
        index: int, 
        offset: int, 
        first_part_cut: int, 
        last_part_cut: int, 
        part_count: int, 
        chunk_size: int
    ):
        client = self.client
        work_loads[index] += 1
        LOGGER.debug(f"Starting to yield file with client {index}. Parts: {part_count}, Offset: {offset}")
        
        media_session = None
        current_part = 1
        total_yielded = 0
        
        try:
            media_session = await self.generate_media_session(client, file_id)
            if not media_session:
                LOGGER.error("Failed to generate media session")
                return
            
            location = await self.get_location(file_id)
            
            while current_part <= part_count:
                try:
                    # Request the chunk
                    r = await media_session.send(
                        raw.functions.upload.GetFile(
                            location=location, 
                            offset=offset, 
                            limit=chunk_size
                        )
                    )
                    
                    if not isinstance(r, raw.types.upload.File):
                        LOGGER.error(f"Unexpected response type: {type(r)}")
                        break
                    
                    chunk = r.bytes
                    if not chunk:
                        LOGGER.warning(f"Empty chunk at part {current_part}/{part_count}, offset {offset}")
                        break
                    
                    # Apply cuts based on part number
                    if part_count == 1:
                        # Single part - cut both ends
                        chunk_to_yield = chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        # First part - cut beginning only
                        chunk_to_yield = chunk[first_part_cut:]
                    elif current_part == part_count:
                        # Last part - cut end only
                        chunk_to_yield = chunk[:last_part_cut]
                    else:
                        # Middle parts - no cuts
                        chunk_to_yield = chunk
                    
                    if chunk_to_yield:
                        yield chunk_to_yield
                        total_yielded += len(chunk_to_yield)
                    
                    current_part += 1
                    offset += chunk_size
                    
                except Exception as e:
                    LOGGER.error(f"Error fetching chunk at part {current_part}, offset {offset}: {e}")
                    break
                    
        except TimeoutError:
            LOGGER.error(f"Timeout while streaming file, yielded {total_yielded} bytes in {current_part-1}/{part_count} parts")
        except AttributeError as e:
            LOGGER.error(f"Attribute error in yield_file: {e}")
        except Exception as e:
            LOGGER.error(f"Unexpected error in yield_file: {e}", exc_info=True)
        finally:
            work_loads[index] -= 1
            LOGGER.debug(f"Finished yielding file. Sent {current_part-1}/{part_count} parts, {total_yielded} bytes")

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        media_session = client.media_sessions.get(file_id.dc_id, None)
        
        if media_session is None:
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(client, file_id.dc_id, await client.storage.test_mode()).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()
                
                # Retry auth export/import up to 6 times
                for attempt in range(6):
                    try:
                        exported_auth = await client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                        )
                        
                        await media_session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, 
                                bytes=exported_auth.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        LOGGER.debug(f"Invalid auth bytes for DC {file_id.dc_id}, attempt {attempt+1}/6")
                        if attempt < 5:
                            await asyncio.sleep(2)
                    except OSError:
                        LOGGER.debug(f"Connection error for DC {file_id.dc_id}, attempt {attempt+1}/6")
                        if attempt < 5:
                            await asyncio.sleep(2)
                else:
                    await media_session.stop()
                    LOGGER.error(f"Failed to establish media session for DC {file_id.dc_id} after 6 retries")
                    return None
            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()
            
            LOGGER.debug(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            LOGGER.debug(f"Using cached media session for DC {file_id.dc_id}")
        
        return media_session

    @staticmethod
    async def get_location(file_id: FileId) -> Union[
        raw.types.InputPhotoFileLocation, 
        raw.types.InputDocumentFileLocation, 
        raw.types.InputPeerPhotoFileLocation
    ]:
        file_type = file_id.file_type
        
        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, 
                    access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id), 
                        access_hash=file_id.chat_access_hash
                    )
            
            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )
        
        return location

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.__cached_file_ids.clear()
            LOGGER.debug("Cleaned the cache")
