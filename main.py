import asyncio, logging, requests
from config import Config
from pyrogram import Client as VJ, idle
from typing import Union, Optional, AsyncGenerator
from logging.handlers import RotatingFileHandler
from plugins.regix import restart_forwards
from aiohttp import ClientSession

# Keep alive function to ping Render URL
async def keep_alive():
    """
    Ping the Render URL every 20 seconds to keep the bot alive
    """
    while True:
        try:
            # Replace with your actual Render URL
            RENDER_URL = ""  # Change this to your actual Render URL
            async with ClientSession() as session:
                async with session.get(RENDER_URL) as response:
                    if response.status == 200:
                        print("Keep-alive ping successful")
                    else:
                        print(f"Keep-alive ping failed with status: {response.status}")
        except Exception as e:
            print(f"Keep-alive error: {e}")
        
        # Wait for 20 seconds before next ping
        await asyncio.sleep(20)

if __name__ == "__main__":
    VJBot = VJ(
        "VJ-Forward-Bot",
        bot_token=Config.BOT_TOKEN,
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        sleep_threshold=120,
        plugins=dict(root="plugins")
    )  
    async def iter_messages(
        self,
        chat_id: Union[int, str],
        limit: int,
        offset: int = 0,
    ) -> Optional[AsyncGenerator["types.Message", None]]:
        """Iterate through a chat sequentially.
        This convenience method does the same as repeatedly calling :meth:`~pyrogram.Client.get_messages` in a loop, thus saving
        you from the hassle of setting up boilerplate code. It is useful for getting the whole chat messages with a
        single call.
        Parameters:
            chat_id (``int`` | ``str``):
                Unique identifier (int) or username (str) of the target chat.
                For your personal cloud (Saved Messages) you can simply use "me" or "self".
                For a contact that exists in your Telegram address book you can use his phone number (str).
                
            limit (``int``):
                Identifier of the last message to be returned.
                
            offset (``int``, *optional*):
                Identifier of the first message to be returned.
                Defaults to 0.
        Returns:
            ``Generator``: A generator yielding :obj:`~pyrogram.types.Message` objects.
        Example:
            .. code-block:: python
                for message in app.iter_messages("pyrogram", 1, 15000):
                    print(message.text)
        """
        current = offset
        while True:
            new_diff = min(200, limit - current)
            if new_diff <= 0:
                return
            messages = await self.get_messages(chat_id, list(range(current, current+new_diff+1)))
            for message in messages:
                yield message
                current += 1
               
    async def main():
        await VJBot.start()
        bot_info  = await VJBot.get_me()
        
        # Start keep-alive task
        keep_alive_task = asyncio.create_task(keep_alive())
        
        await restart_forwards(VJBot)
        print("Bot Started with keep-alive functionality.")
        await idle()
        
        # Cancel keep-alive task when bot stops
        keep_alive_task.cancel()

    asyncio.get_event_loop().run_until_complete(main())
