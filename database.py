import motor.motor_asyncio
from config import Config
from typing import List, Dict, Tuple, Union, Optional, Any

class Db:

    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.bot = self.db.bots
        self.userbot = self.db.userbot 
        self.col = self.db.users
        self.nfy = self.db.notify
        self.chl = self.db.channels 

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )

    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self.col.insert_one(user)

    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        return bool(user)

    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count

    async def total_users_bots_count(self):
        bcount = await self.bot.count_documents({})
        count = await self.col.count_documents({})
        return count, bcount

    async def remove_ban(self, id):
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})

    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def get_ban_status(self, id):
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = await self.col.find_one({'id':int(id)})
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_all_users(self):
        return self.col.find({})

    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})

    async def get_banned(self):
        users = self.col.find({'ban_status.is_banned': True})
        b_users = [user['id'] async for user in users]
        return b_users

    async def update_configs(self, id, configs):
        await self.col.update_one({'id': int(id)}, {'$set': {'configs': configs}})

    async def get_configs(self, id):
        default = {
            'caption': None,
            'duplicate': True,
            'forward_tag': False,
            'min_size': 0,
            'max_size': 0,
            'replace_rules': [],  # Add this line
            'extension': None,
            'keywords': None,
            'protect': None,
            'button': None,
            'db_uri': None,
            'filters': {
               'poll': True,
               'text': True,
               'audio': True,
               'voice': True,
               'video': True,
               'photo': True,
               'document': True,
               'animation': True,
               'sticker': True
            }
        }
        user = await self.col.find_one({'id':int(id)})
        if user:
            return user.get('configs', default)
        return default 

    async def add_bot(self, datas):
       if not await self.is_bot_exist(datas['user_id']):
          await self.bot.insert_one(datas)

    async def remove_bot(self, user_id):
       await self.bot.delete_many({'user_id': int(user_id)})

    async def get_bot(self, user_id: int):
       bot = await self.bot.find_one({'user_id': user_id})
       return bot if bot else None

    async def is_bot_exist(self, user_id):
       bot = await self.bot.find_one({'user_id': user_id})
       return bool(bot)
   
    async def add_userbot(self, datas):
       if not await self.is_userbot_exist(datas['user_id']):
          await self.userbot.insert_one(datas)

    async def remove_userbot(self, user_id):
       await self.userbot.delete_many({'user_id': int(user_id)})

    async def get_userbot(self, user_id: int):
       bot = await self.userbot.find_one({'user_id': user_id})
       return bot if bot else None

    async def is_userbot_exist(self, user_id):
       bot = await self.userbot.find_one({'user_id': user_id})
       return bool(bot)
    
    async def in_channel(self, user_id: int, chat_id: int) -> bool:
       channel = await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})
       return bool(channel)

    async def add_channel(self, user_id: int, chat_id: int, title, username):
       channel = await self.in_channel(user_id, chat_id)
       if channel:
         return False
       return await self.chl.insert_one({"user_id": user_id, "chat_id": chat_id, "title": title, "username": username})

    async def remove_channel(self, user_id: int, chat_id: int):
       channel = await self.in_channel(user_id, chat_id )
       if not channel:
         return False
       return await self.chl.delete_many({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_channel_details(self, user_id: int, chat_id: int):
       return await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_user_channels(self, user_id: int):
       channels = self.chl.find({"user_id": int(user_id)})
       return [channel async for channel in channels]

    async def get_filters(self, user_id):
       filters = []
       filter = (await self.get_configs(user_id))['filters']
       for k, v in filter.items():
          if v == False:
            filters.append(str(k))
       return filters

    async def add_frwd(self, user_id):
       return await self.nfy.insert_one({'user_id': int(user_id)})

    async def rmve_frwd(self, user_id=0, all=False):
       data = {} if all else {'user_id': int(user_id)}
       return await self.nfy.delete_many(data)

    async def get_all_frwd(self):
       return self.nfy.find({})
  
    async def forwad_count(self):
        c = await self.nfy.count_documents({})
        return c
        
    async def is_forwad_exit(self, user):
        u = await self.nfy.find_one({'user_id': user})
        return bool(u)
        
    async def get_forward_details(self, user_id):
        defult = {
            'chat_id': None,
            'forward_id': None,
            'toid': None,
            'last_id': None,
            'limit': None,
            'msg_id': None,
            'start_time': None,
            'fetched': 0,
            'offset': 0,
            'deleted': 0,
            'total': 0,
            'duplicate': 0,
            'skip': 0,
            'filtered' :0
        }
        user = await self.nfy.find_one({'user_id': int(user_id)})
        if user:
            return user.get('details', defult)
        return defult
       # Add these methods to your Db class
    
    async def add_to_queue(self, user_id: int, queue_data: dict):
        """Add task to queue"""
        query = """
        INSERT INTO user_queue (user_id, queue_id, data, status, priority, added_time)
        VALUES ($1, $2, $3, $4, $5, $6)
        """
        await self.con.execute(
            query, user_id, queue_data['queue_id'], 
            json.dumps(queue_data), 'pending',
            queue_data.get('priority', 'normal'), time.time()
        )
    
    async def get_user_queue(self, user_id: int) -> List[dict]:
        """Get user's queue"""
        query = "SELECT data FROM user_queue WHERE user_id = $1 ORDER BY added_time"
        rows = await self.con.fetch(query, user_id)
        return [json.loads(row['data']) for row in rows]
    
    async def update_queue_status(self, user_id: int, queue_id: str, status: str):
        """Update queue status"""
        query = "UPDATE user_queue SET status = $1 WHERE user_id = $2 AND queue_id = $3"
        await self.con.execute(query, status, user_id, queue_id)
    
    async def remove_from_queue(self, user_id: int, queue_id: str) -> bool:
        """Remove from queue"""
        query = "DELETE FROM user_queue WHERE user_id = $1 AND queue_id = $2"
        await self.con.execute(query, user_id, queue_id)
        return True
    
    async def clear_user_queue(self, user_id: int) -> bool:
        """Clear user's queue"""
        query = "DELETE FROM user_queue WHERE user_id = $1"
        await self.con.execute(query, user_id)
        return True
        # Add these new methods for replace rules
    async def get_replace_rules(self, user_id: int):
        """Get user's replace rules"""
        config = await self.get_configs(user_id)
        return config.get('replace_rules', [])

    async def update_replace_rules(self, user_id: int, rules: list):
        """Update user's replace rules"""
        await self.update_config_field(user_id, 'replace_rules', rules)
        return True

    async def clear_replace_rules(self, user_id: int):
        """Clear user's replace rules"""
        await self.update_config_field(user_id, 'replace_rules', [])
        return True

    async def add_schedule(self, user_id: int, schedule_data: dict):
        """Add scheduled task"""
        query = """
        INSERT INTO user_schedule (user_id, schedule_id, data, scheduled_time, repeat)
        VALUES ($1, $2, $3, $4, $5)
        """
        await self.con.execute(
            query, user_id, schedule_data['schedule_id'], 
            json.dumps(schedule_data), schedule_data['scheduled_time'],
            schedule_data.get('repeat', 'none')
        )
    
    # Also add table creation SQL:
    """
    CREATE TABLE IF NOT EXISTS user_queue (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        queue_id VARCHAR(100) NOT NULL,
        data JSONB NOT NULL,
        status VARCHAR(20) DEFAULT 'pending',
        priority VARCHAR(10) DEFAULT 'normal',
        added_time DOUBLE PRECISION NOT NULL,
        completed_time DOUBLE PRECISION
    );
    
    CREATE TABLE IF NOT EXISTS user_schedule (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        schedule_id VARCHAR(100) NOT NULL,
        data JSONB NOT NULL,
        scheduled_time DOUBLE PRECISION NOT NULL,
        repeat VARCHAR(20) DEFAULT 'none',
        last_run DOUBLE PRECISION,
        enabled BOOLEAN DEFAULT TRUE
    );
    """
    async def update_forward(self, user_id, details):
        await self.nfy.update_one({'user_id': user_id}, {'$set': {'details': details}})
        
db = Db(Config.DATABASE_URI, Config.DATABASE_NAME)
