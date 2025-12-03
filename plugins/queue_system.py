import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from database import db
from config import temp

class QueueSystem:
    def __init__(self):
        self.active_queues = {}
        self.queue_lock = asyncio.Lock()
        
    async def add_to_queue(self, user_id: int, queue_data: dict) -> str:
        """Add a forwarding task to queue"""
        async with self.queue_lock:
            queue_id = f"{user_id}_{int(time.time())}"
            queue_data.update({
                'queue_id': queue_id,
                'status': 'pending',
                'added_time': time.time(),
                'priority': queue_data.get('priority', 'normal')
            })
            
            await db.add_to_queue(user_id, queue_data)
            
            # Add to active queues if smart forward is enabled
            user_config = await db.get_configs(user_id)
            if user_config.get('smart_forward', False):
                if user_id not in self.active_queues:
                    self.active_queues[user_id] = []
                self.active_queues[user_id].append(queue_data)
            
            return queue_id
    
    async def get_queue_status(self, user_id: int) -> List[dict]:
        """Get user's queue status"""
        return await db.get_user_queue(user_id)
    
    async def remove_from_queue(self, user_id: int, queue_id: str) -> bool:
        """Remove task from queue"""
        async with self.queue_lock:
            if user_id in self.active_queues:
                self.active_queues[user_id] = [
                    q for q in self.active_queues[user_id] 
                    if q['queue_id'] != queue_id
                ]
            return await db.remove_from_queue(user_id, queue_id)
    
    async def clear_user_queue(self, user_id: int) -> bool:
        """Clear all user's queue"""
        async with self.queue_lock:
            if user_id in self.active_queues:
                del self.active_queues[user_id]
            return await db.clear_user_queue(user_id)
    
    async def process_smart_forward(self, user_id: int):
        """Process smart forwarding based on rules"""
        if user_id not in self.active_queues:
            return
        
        user_config = await db.get_configs(user_id)
        smart_rules = user_config.get('smart_rules', {})
        
        # Sort by priority
        self.active_queues[user_id].sort(
            key=lambda x: {'high': 0, 'normal': 1, 'low': 2}.get(x['priority'], 1)
        )
        
        for queue_item in self.active_queues[user_id]:
            if queue_item['status'] == 'pending':
                # Apply smart rules
                if smart_rules.get('skip_peak_hours', False):
                    hour = datetime.now().hour
                    if 20 <= hour <= 23 or 0 <= hour <= 2:  # Peak hours 8PM-2AM
                        continue
                
                if smart_rules.get('delay_between_files', 0) > 0:
                    await asyncio.sleep(smart_rules['delay_between_files'])
                
                # Process this item
                queue_item['status'] = 'processing'
                await self._process_queue_item(user_id, queue_item)
    
    async def _process_queue_item(self, user_id: int, queue_item: dict):
        """Process individual queue item"""
        try:
            # Extract data from queue
            from_chat = queue_item['from_chat']
            to_chat = queue_item['to_chat']
            message_ids = queue_item.get('message_ids', [])
            skip = queue_item.get('skip', 0)
            limit = queue_item.get('limit', 0)
            
            # Get user's bot/client
            user_config = await db.get_configs(user_id)
            bot_data = user_config.get('bot_data')
            
            if not bot_data:
                queue_item['status'] = 'failed'
                return
            
            # Process the forwarding (you'll need to integrate with your existing forwarding logic)
            # This is a placeholder - integrate with your actual forwarding function
            success = await self._execute_forward(
                user_id, bot_data, from_chat, to_chat, 
                message_ids, skip, limit, queue_item
            )
            
            queue_item['status'] = 'completed' if success else 'failed'
            queue_item['completed_time'] = time.time()
            
        except Exception as e:
            print(f"Error processing queue item: {e}")
            queue_item['status'] = 'failed'
        
        finally:
            await db.update_queue_status(user_id, queue_item['queue_id'], queue_item['status'])

# Initialize queue system
queue_system = QueueSystem()

# Add to your existing handlers
@Client.on_callback_query(filters.regex(r'^settings#queue$'))
async def queue_settings(bot, query):
    user_id = query.from_user.id
    buttons = [
        [InlineKeyboardButton('📋 View Queue', callback_data='view_queue')],
        [InlineKeyboardButton('🗑️ Clear Queue', callback_data='clear_queue')],
        [InlineKeyboardButton('⚙️ Queue Settings', callback_data='queue_config')],
        [InlineKeyboardButton('⫷ Back', callback_data='settings#main')]
    ]
    
    await query.message.edit_text(
        """<b>🗂️ Queue Management</b>
        
Manage your forwarding queue system.
Queue allows you to schedule multiple forwarding tasks."""
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@Client.on_callback_query(filters.regex(r'^settings#smart_forward$'))
async def smart_forward_settings(bot, query):
    user_id = query.from_user.id
    user_config = await db.get_configs(user_id)
    smart_rules = user_config.get('smart_rules', {})
    
    is_enabled = smart_rules.get('enabled', False)
    skip_peak = smart_rules.get('skip_peak_hours', False)
    delay = smart_rules.get('delay_between_files', 0)
    
    buttons = [
        [
            InlineKeyboardButton(
                f"Smart Forward: {'✅ ON' if is_enabled else '❌ OFF'}",
                callback_data=f'toggle_smart_{"off" if is_enabled else "on"}'
            )
        ],
        [
            InlineKeyboardButton(
                f"Skip Peak Hours: {'✅' if skip_peak else '❌'}",
                callback_data=f'toggle_peak_{"off" if skip_peak else "on"}'
            )
        ],
        [
            InlineKeyboardButton(
                f"Delay: {delay}s",
                callback_data='set_delay'
            )
        ],
        [InlineKeyboardButton('⫷ Back', callback_data='settings#main')]
    ]
    
    await query.message.edit_text(
        """<b>🔄 Smart Forwarding</b>
        
Smart features to optimize your forwarding:
• Skip peak hours (8PM-2AM)
• Automatic delay between files
• Priority-based processing"""
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@Client.on_callback_query(filters.regex(r'^settings#schedule$'))
async def schedule_settings(bot, query):
    user_id = query.from_user.id
    buttons = [
        [InlineKeyboardButton('⏰ Schedule Forward', callback_data='schedule_new')],
        [InlineKeyboardButton('📅 View Schedule', callback_data='view_schedule')],
        [InlineKeyboardButton('⫷ Back', callback_data='settings#main')]
    ]
    
    await query.message.edit_text(
        """<b>⏰ Scheduling</b>
        
Schedule forwarding tasks for specific times.
Features:
• One-time scheduling
• Recurring schedules
• Timezone support"""
        reply_markup=InlineKeyboardMarkup(buttons)
    )
