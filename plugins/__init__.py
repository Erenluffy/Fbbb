# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01

import asyncio
from config import Config
from .dump_channel import dump_manager

async def init_all_plugins(bot):
    """Initialize all plugins including dump manager"""
    # Initialize dump manager
    try:
        await dump_manager.initialize_bot(Config.BOT_TOKEN)
        print("✅ Dump channel initialized successfully")
    except Exception as e:
        print(f"⚠️ Failed to initialize dump channel: {e}")
    
    # You can add other plugin initializations here
    
async def cleanup_all_plugins():
    """Cleanup all plugins"""
    await dump_manager.stop()

# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01
