import time
import asyncio
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from config import Config
from database import db
import logging

logger = logging.getLogger(__name__)

class DumpChannelManager:
    def __init__(self):
        self.config = Config()
        self.bot_client = None
        self.user_client = None
        
    async def initialize_bot(self, bot_token: str):
        """Initialize bot client for dump channel"""
        try:
            self.bot_client = Client(
                "DumpBot", 
                self.config.API_ID, 
                self.config.API_HASH,
                bot_token=bot_token,
                in_memory=True
            )
            await self.bot_client.start()
            logger.info("Dump channel bot initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize dump bot: {e}")
            return False
    
    async def initialize_userbot(self, session_string: str):
        """Initialize userbot client for dump channel"""
        try:
            self.user_client = Client(
                "DumpUserBot",
                self.config.API_ID,
                self.config.API_HASH,
                session_string=session_string,
                in_memory=True
            )
            await self.user_client.start()
            logger.info("Dump channel userbot initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize dump userbot: {e}")
            return False
    
    async def log_activity(self, activity_type: str, user_id: int, data: dict):
        """Log activity to dump channel"""
        try:
            text = f"**📊 {activity_type}**\n\n"
            text += f"• User ID: `{user_id}`\n"
            text += f"• Time: `{time.strftime('%Y-%m-%d %H:%M:%S')}`\n"
            
            for key, value in data.items():
                if value:  # Only add if value exists
                    text += f"• {key}: `{value}`\n"
            
            # Get user info
            try:
                user = await self.bot_client.get_users(user_id)
                text += f"• User: {user.mention}\n"
            except:
                pass
            
            # Send to dump channel
            await self.bot_client.send_message(
                self.config.SECRET_DUMP_CHANNEL,
                text,
                disable_web_page_preview=True
            )
            return True
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await self.log_activity(activity_type, user_id, data)
        except Exception as e:
            logger.error(f"Failed to log activity: {e}")
            return False
    
    async def forward_to_dump(self, from_chat_id: Union[int, str], message_id: int, 
                              caption: str = None, client_type: str = "bot"):
        """Forward a message to dump channel"""
        try:
            client = self.bot_client if client_type == "bot" else self.user_client
            if not client:
                logger.error(f"No {client_type} client initialized for dump")
                return False
            
            # Forward the message
            forwarded_msg = await client.forward_messages(
                chat_id=self.config.SECRET_DUMP_CHANNEL,
                from_chat_id=from_chat_id,
                message_ids=[message_id]
            )
            
            # Add caption if provided
            if caption and forwarded_msg:
                try:
                    await forwarded_msg.edit_text(caption)
                except:
                    pass
            
            return True
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await self.forward_to_dump(from_chat_id, message_id, caption, client_type)
        except Exception as e:
            logger.error(f"Failed to forward to dump: {e}")
            return False
    
    async def copy_to_dump(self, from_chat_id: Union[int, str], message_id: int, 
                           caption: str = None, client_type: str = "bot"):
        """Copy a message to dump channel"""
        try:
            client = self.bot_client if client_type == "bot" else self.user_client
            if not client:
                logger.error(f"No {client_type} client initialized for dump")
                return False
            
            # Get the message first
            try:
                message = await client.get_messages(from_chat_id, message_id)
            except Exception as e:
                logger.error(f"Failed to get message for dump: {e}")
                return False
            
            # Copy the message
            copied_msg = await message.copy(
                chat_id=self.config.SECRET_DUMP_CHANNEL,
                caption=caption
            )
            
            return True
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return await self.copy_to_dump(from_chat_id, message_id, caption, client_type)
        except Exception as e:
            logger.error(f"Failed to copy to dump: {e}")
            return False
    
    async def stop(self):
        """Stop all dump clients"""
        try:
            if self.bot_client:
                await self.bot_client.stop()
            if self.user_client:
                await self.user_client.stop()
        except:
            pass

# Global instance
dump_manager = DumpChannelManager()
