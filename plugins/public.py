import re
import asyncio 
import time
import math
from .utils import STS
from database import Db, db
from config import temp, Config
from script import Script
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.errors.exceptions.not_acceptable_406 import ChannelPrivate as PrivateChat
from pyrogram.errors.exceptions.bad_request_400 import ChannelInvalid, ChatAdminRequired, UsernameInvalid, UsernameNotModified, ChannelPrivate
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Message

# Import dump manager
from plugins.dump_channel import dump_manager
from plugins.test import CLIENT, get_client, iter_messages, parse_buttons

CLIENT = CLIENT()

# Initialize dump manager with main bot
async def init_dump_manager():
    """Initialize dump manager with main bot token"""
    config = Config()
    await dump_manager.initialize_bot(config.BOT_TOKEN)

# Helper functions
def get_size(size):
    """Convert bytes to readable format"""
    units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB"]
    size = float(size)
    i = 0
    while size >= 1024.0 and i < len(units):
        i += 1
        size /= 1024.0
    return "%.2f %s" % (size, units[i])

def TimeFormatter(milliseconds: int) -> str:
    """Format milliseconds to readable time"""
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = ((str(days) + "d, ") if days else "") + \
        ((str(hours) + "h, ") if hours else "") + \
        ((str(minutes) + "m, ") if minutes else "") + \
        ((str(seconds) + "s, ") if seconds else "") + \
        ((str(milliseconds) + "ms, ") if milliseconds else "")
    return tmp[:-2]

def media(msg):
    """Extract media file_id from message"""
    if msg.media:
        media_obj = getattr(msg, msg.media.value, None)
        if media_obj:
            return getattr(media_obj, 'file_id', None)
    return None

def retry_btn(id):
    """Create retry button"""
    return InlineKeyboardMarkup([[InlineKeyboardButton('♻️ RETRY ♻️', f"start_public_{id}")]])

async def msg_edit(msg, text, button=None, wait=None):
    """Edit message with error handling"""
    try:
        return await msg.edit(text, reply_markup=button)
    except MessageNotModified:
        pass 
    except FloodWait as e:
        if wait:
            await asyncio.sleep(e.value)
            return await msg_edit(msg, text, button, wait)

async def send(bot, user, text):
    """Send message to user"""
    try:
        await bot.send_message(user, text=text)
    except:
        pass

async def stop(client, user):
    """Stop client and cleanup"""
    try:
        await client.stop()
    except:
        pass 
    await db.rmve_frwd(user)
    temp.forwardings -= 1
    temp.lock[user] = False

async def is_cancelled(client, user, msg, sts):
    """Check if forwarding is cancelled"""
    if temp.CANCEL.get(user) == True:
        if sts.TO in temp.IS_FRWD_CHAT:
            temp.IS_FRWD_CHAT.remove(sts.TO)
        
        # LOG: User cancelled
        if dump_manager.bot_client:
            await dump_manager.log_activity("USER CANCELLED", user, {
                'From Chat': sts.get("FROM"),
                'To Chat': sts.get("TO"),
                'Forward ID': sts.id,
                'Files Processed': sts.get('fetched'),
                'Files Forwarded': sts.get('total_files')
            })
        
        await edit(user, msg, 'ᴄᴀɴᴄᴇʟʟᴇᴅ', "cancelled", sts)
        await send(client, user, "<b>❌ ғᴏʀᴡᴀʀᴅɪɴɢ ᴄᴀɴᴄᴇʟʟᴇᴅ</b>")
        await stop(client, user)
        return True 
    return False

def custom_caption(msg, caption, replace_rules=None):
    """Apply custom caption and replace rules"""
    if msg.media:
        if (msg.video or msg.document or msg.audio or msg.photo):
            media_obj = getattr(msg, msg.media.value, None)
            if media_obj:
                file_name = getattr(media_obj, 'file_name', '')
                file_size = getattr(media_obj, 'file_size', '')
                fcaption = getattr(msg, 'caption', '')
                
                if fcaption:
                    fcaption = fcaption.html
                
                # Apply custom caption template
                if caption:
                    new_caption = caption.format(
                        filename=file_name, 
                        size=get_size(file_size), 
                        caption=fcaption
                    )
                else:
                    new_caption = fcaption
                
                # Apply replace rules if any
                if replace_rules and new_caption:
                    for rule in replace_rules:
                        old_text = rule.get('old', '')
                        new_text = rule.get('new', '')
                        if old_text and new_text and old_text in new_caption:
                            new_caption = new_caption.replace(old_text, new_text)
                
                return new_caption
    return None

async def keyword_filter(keywords, file_name):
    """Filter by keywords"""
    if keywords is None:
        return False
    if re.search(keywords, file_name, re.IGNORECASE):
        return False
    else:
        return True

async def extension_filter(extensions, file_name):
    """Filter by extensions"""
    if extensions is None:
        return False
    if not re.search(extensions, file_name, re.IGNORECASE):
        return False
    else:
        return True

async def size_filter(max_size, min_size, file_size):
    """Filter by size"""
    if file_size is None:
        return False
        
    file_size_mb = file_size / 1024 / 1024
    
    if max_size == 0 and min_size == 0:
        return False
    
    if max_size == 0:
        return file_size_mb < min_size
    
    if min_size == 0:
        return file_size_mb > max_size
    
    if not min_size <= file_size_mb <= max_size:
        return True
    else:
        return False

async def copy(user, bot, msg, m, sts):
    """Copy message to destination"""
    try:                               
        if msg.get("media") and msg.get("caption"):
            result = await bot.send_cached_media(
                chat_id=sts.get('TO'),
                file_id=msg.get("media"),
                caption=msg.get("caption"),
                reply_markup=msg.get('button'),
                protect_content=msg.get("protect")
            )
        else:
            result = await bot.copy_message(
                chat_id=sts.get('TO'),
                from_chat_id=sts.get('FROM'),    
                caption=msg.get("caption"),
                message_id=msg.get("msg_id"),
                reply_markup=msg.get('button'),
                protect_content=msg.get("protect")
            )
        return result
    except FloodWait as e:
        # LOG: Flood wait
        if dump_manager.bot_client:
            await dump_manager.log_activity("FLOOD WAIT", user, {
                'From Chat': sts.get("FROM"),
                'To Chat': sts.get("TO"),
                'Wait Time': f"{e.value} seconds",
                'Message ID': msg.get("msg_id")
            })
        
        await edit(user, m, 'ᴘʀᴏɢʀᴇssɪɴɢ', e.value, sts)
        await asyncio.sleep(e.value)
        await edit(user, m, 'ᴘʀᴏɢʀᴇssɪɴɢ', 5, sts)
        return await copy(user, bot, msg, m, sts)
    except Exception as e:
        sts.add('deleted')
        
        # LOG: Copy error
        if dump_manager.bot_client:
            await dump_manager.log_activity("COPY ERROR", user, {
                'From Chat': sts.get("FROM"),
                'To Chat': sts.get("TO"),
                'Message ID': msg.get("msg_id"),
                'Error': str(e)[:100]
            })
        
        raise e

async def forward(user, bot, msg, m, sts, protect):
    """Forward messages in bulk"""
    try:                             
        result = await bot.forward_messages(
            chat_id=sts.get('TO'),
            from_chat_id=sts.get('FROM'), 
            protect_content=protect,
            message_ids=msg
        )
        return result
    except FloodWait as e:
        # LOG: Flood wait
        if dump_manager.bot_client:
            await dump_manager.log_activity("FLOOD WAIT", user, {
                'From Chat': sts.get("FROM"),
                'To Chat': sts.get("TO"),
                'Wait Time': f"{e.value} seconds",
                'Messages': len(msg)
            })
        
        await edit(user, m, 'ᴘʀᴏɢʀᴇssɪɴɢ', e.value, sts)
        await asyncio.sleep(e.value)
        await edit(user, m, 'ᴘʀᴏɢʀᴇssɪɴɢ', 5, sts)
        return await forward(user, bot, msg, m, sts, protect)
    except Exception as e:
        # LOG: Forward error
        if dump_manager.bot_client:
            await dump_manager.log_activity("FORWARD ERROR", user, {
                'From Chat': sts.get("FROM"),
                'To Chat': sts.get("TO"),
                'Messages': len(msg),
                'Error': str(e)[:100]
            })
        
        raise e

async def edit(user, msg, title, status, sts):
    """Update progress message"""
    i = sts.get(full=True)
    status = 'Forwarding' if status == 5 else f"sleeping {status} s" if str(status).isnumeric() else status
    percentage = "{:.0f}".format(float(i.fetched)*100/float(i.total) if i.total > 0 else 0)
    
    text = Script.TEXT.format(
        i.fetched, i.total_files, i.duplicate, i.deleted, 
        i.skip, i.filtered, status, percentage, title
    )
    
    # Update database
    await update_forward(
        user_id=user, last_id=None, start_time=i.start, 
        limit=i.limit, chat_id=i.FROM, toid=i.TO, 
        forward_id=None, msg_id=msg.id, fetched=i.fetched, 
        deleted=i.deleted, total=i.total_files, 
        duplicate=i.duplicate, skip=i.skip, filterd=i.filtered
    )
    
    now = time.time()
    diff = int(now - i.start)
    speed = sts.divide(i.fetched, diff) if diff > 0 else 0
    elapsed_time = round(diff) * 1000
    time_to_completion = round(sts.divide(i.total - i.fetched, int(speed))) * 1000 if speed > 0 else 0
    estimated_total_time = elapsed_time + time_to_completion  
    
    progress = "●{0}{1}".format(
        ''.join(["●" for i in range(math.floor(int(percentage) / 4))]),
        ''.join(["○" for i in range(24 - math.floor(int(percentage) / 4))]))
    
    button = [[InlineKeyboardButton(progress, f'fwrdstatus#{status}#{estimated_total_time}#{percentage}#{sts.id}')]]
    estimated_total_time = TimeFormatter(milliseconds=estimated_total_time)
    estimated_total_time = estimated_total_time if estimated_total_time != '' else '0 s'
    
    if status in ["cancelled", "completed"]:
        button.append([InlineKeyboardButton('• ᴄᴏᴍᴘʟᴇᴛᴇᴅ ​•', url='https://t.me/SteveBotz')])
    else:
        button.append([InlineKeyboardButton('• ᴄᴀɴᴄᴇʟ', 'terminate_frwd')])
    
    await msg_edit(msg, text, InlineKeyboardMarkup(button))

async def update_forward(user_id, chat_id, start_time, toid, last_id, limit, forward_id, msg_id, fetched, total, duplicate, deleted, skip, filterd):
    """Update forward progress in database"""
    details = {
        'chat_id': chat_id,
        'toid': toid,
        'forward_id': forward_id,
        'last_id': last_id,
        'limit': limit,
        'msg_id': msg_id,
        'start_time': start_time,
        'fetched': fetched,
        'offset': fetched,
        'deleted': deleted,
        'total': total,
        'duplicate': duplicate,
        'skip': skip,
        'filtered': filterd
    }
    await db.update_forward(user_id, details)

@Client.on_message(filters.private & filters.command(["forward"]))
async def run(bot, message):
    buttons = []
    btn_data = {}
    user_id = message.from_user.id
    _bot = await db.get_bot(user_id)
    if not _bot:
        _bot = await db.get_userbot(user_id)
        if not _bot:
            return await message.reply("<code>You didn't added any bot. Please add a bot using /settings !</code>")
    
    channels = await db.get_user_channels(user_id)
    if not channels:
        return await message.reply_text("please set a to channel in /settings before forwarding")
    
    if len(channels) > 1:
        for channel in channels:
            buttons.append([KeyboardButton(f"{channel['title']}")])
            btn_data[channel['title']] = channel['chat_id']
        buttons.append([KeyboardButton("cancel")]) 
        _toid = await bot.ask(
            message.chat.id, 
            Script.TO_MSG.format(_bot['name'], _bot['username']), 
            reply_markup=ReplyKeyboardMarkup(buttons, one_time_keyboard=True, resize_keyboard=True)
        )
        if _toid.text.startswith(('/', 'cancel')):
            return await message.reply_text(Script.CANCEL, reply_markup=ReplyKeyboardRemove())
        to_title = _toid.text
        toid = btn_data.get(to_title)
        if not toid:
            return await message.reply_text("wrong channel choosen !", reply_markup=ReplyKeyboardRemove())
    else:
        toid = channels[0]['chat_id']
        to_title = channels[0]['title']
    
    fromid = await bot.ask(message.chat.id, Script.FROM_MSG, reply_markup=ReplyKeyboardRemove())
    if fromid.text and fromid.text.startswith('/'):
        await message.reply(Script.CANCEL)
        return 
    
    if fromid.text and not fromid.forward_date:
        regex = re.compile("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
        match = regex.match(fromid.text.replace("?single", ""))
        if not match:
            return await message.reply('Invalid link')
        chat_id = match.group(4)
        last_msg_id = int(match.group(5))
        if chat_id.isnumeric():
            chat_id  = int(("-100" + chat_id))
    elif fromid.forward_from_chat.type in [enums.ChatType.CHANNEL, 'supergroup']:
        last_msg_id = fromid.forward_from_message_id
        chat_id = fromid.forward_from_chat.username or fromid.forward_from_chat.id
        if last_msg_id == None:
            return await message.reply_text("**This may be a forwarded message from a group and sended by anonymous admin. instead of this please send last message link from group**")
    else:
        await message.reply_text("**invalid !**")
        return 
    
    try:
        title = (await bot.get_chat(chat_id)).title
    except (PrivateChat, ChannelPrivate, ChannelInvalid):
        title = "private" if fromid.text else fromid.forward_from_chat.title
    except (UsernameInvalid, UsernameNotModified):
        return await message.reply('Invalid Link specified.')
    except Exception as e:
        return await message.reply(f'Errors - {e}')
    
    skipno = await bot.ask(message.chat.id, Script.SKIP_MSG)
    if skipno.text.startswith('/'):
        await message.reply(Script.CANCEL)
        return
    
    forward_id = f"{user_id}-{skipno.id}"
    buttons = [[
        InlineKeyboardButton('Yes', callback_data=f"start_public_{forward_id}"),
        InlineKeyboardButton('No', callback_data="close_btn")
    ]]
    reply_markup = InlineKeyboardMarkup(buttons)
    
    await message.reply_text(
        text=Script.DOUBLE_CHECK.format(
            botname=_bot['name'], botuname=_bot['username'], 
            from_chat=title, to_chat=to_title, skip=skipno.text
        ),
        disable_web_page_preview=True,
        reply_markup=reply_markup
    )
    STS(forward_id).store(chat_id, toid, int(skipno.text), int(last_msg_id))

@Client.on_callback_query(filters.regex(r'^start_public'))
async def pub_(bot, message):
    user = message.from_user.id
    temp.CANCEL[user] = False
    frwd_id = message.data.split("_")[2]
    
    if temp.lock.get(user) and str(temp.lock.get(user)) == "True":
        return await message.answer("please wait until previous task complete", show_alert=True)
    
    sts = STS(frwd_id)
    if not sts.verify():
        await message.answer("your are clicking on my old button", show_alert=True)
        return await message.message.delete()
    
    i = sts.get(full=True)
    
    if i.TO in temp.IS_FRWD_CHAT:
        return await message.answer("In Target chat a task is progressing. please wait until task complete", show_alert=True)
    
    m = await msg_edit(message.message, "<code>verifying your data's, please wait.</code>")
    
    # Initialize dump manager if not already
    if not dump_manager.bot_client:
        await init_dump_manager()
    
    _bot, caption, forward_tag, datas, protect, button = await sts.get_data(user)
    
    # Get replace rules from user config
    user_config = await db.get_configs(user)
    replace_rules = user_config.get('replace_rules', [])
    
    filter = datas['filters']
    max_size = datas['max_size']
    min_size = datas['min_size']
    keyword = datas['keywords']
    exten = datas['extensions']
    
    keywords = ""
    extensions = ""
    if keyword:
        for key in keyword:
            keywords += f"{key}|"
        keywords = keywords.rstrip("|")
    else:
        keywords = None
    
    if exten:
        for ext in exten:
            extensions += f"{ext}|"
        extensions = extensions.rstrip("|")
    else:
        extensions = None
    
    if not _bot:
        return await msg_edit(m, "<code>You didn't added any bot. Please add a bot using /settings !</code>", wait=True)
    
    if _bot['is_bot'] == True:
        data = _bot['token']
        client_type = "bot"
    else:
        data = _bot['session']
        client_type = "userbot"
    
    try:
        il = True if _bot['is_bot'] == True else False
        client = await get_client(data, is_bot=il)
        await client.start()
    except Exception as e:  
        return await m.edit(str(e))
    
    await msg_edit(m, "<code>processing..</code>")
    
    # LOG: Forwarding started
    if dump_manager.bot_client:
        await dump_manager.log_activity("FORWARD STARTED", user, {
            'From Chat': i.FROM,
            'To Chat': i.TO,
            'Bot': _bot['name'],
            'Skip': i.skip,
            'Limit': i.limit,
            'Forward ID': frwd_id,
            'Replace Rules': f"{len(replace_rules)} rules" if replace_rules else "None"
        })
    
    try: 
        await client.get_messages(sts.get("FROM"), sts.get("limit"))
    except:
        await msg_edit(m, f"**Source chat may be a private channel / group. Use userbot (user must be member over there) or if Make Your [Bot](t.me/{_bot['username']}) an admin over there**", retry_btn(frwd_id), True)
        
        # LOG: Error - Cannot access source
        if dump_manager.bot_client:
            await dump_manager.log_activity("ERROR - SOURCE ACCESS", user, {
                'From Chat': i.FROM,
                'Error': 'Cannot access source chat'
            })
        
        return await stop(client, user)
    
    try:
        k = await client.send_message(i.TO, "Testing")
        await k.delete()
    except:
        await msg_edit(m, f"**Please Make Your [UserBot / Bot](t.me/{_bot['username']}) Admin In Target Channel With Full Permissions**", retry_btn(frwd_id), True)
        
        # LOG: Error - Cannot access target
        if dump_manager.bot_client:
            await dump_manager.log_activity("ERROR - TARGET ACCESS", user, {
                'To Chat': i.TO,
                'Error': 'Cannot access target chat'
            })
        
        return await stop(client, user)
    
    user_have_db = False
    dburi = datas['db_uri']
    if dburi is not None:
        connected, user_db = await connect_user_db(user, dburi, i.TO)
        if not connected:
            await msg_edit(m, "<code>Cannot Connected Your db Errors Found Dup files Have Been Skipped after Restart</code>")
        else:
            user_have_db = True
    
    temp.forwardings += 1
    await db.add_frwd(user)
    
    await send(client, user, "<b>Fᴏʀᴡᴀʀᴅɪɴɢ sᴛᴀʀᴛᴇᴅ 🌼</b>")
    
    sts.add(time=True)
    sleep = 1 if _bot['is_bot'] else 10
    
    await msg_edit(m, "<code>processing...</code>") 
    
    temp.IS_FRWD_CHAT.append(i.TO)
    temp.lock[user] = locked = True
    dup_files = []
    
    if locked:
        try:
            MSG = []
            pling = 0
            await edit(user, m, 'ᴘʀᴏɢʀᴇssɪɴɢ', 5, sts)
            
            async for message_obj in iter_messages(client, chat_id=sts.get("FROM"), limit=sts.get("limit"), offset=sts.get("skip"), filters=filter, max_size=max_size):
                if await is_cancelled(client, user, m, sts):
                    if user_have_db:
                        await user_db.drop_all()
                        await user_db.close()
                    return
                
                if pling % 20 == 0: 
                    await edit(user, m, 'ᴘʀᴏɢʀᴇssɪɴɢ', 5, sts)
                
                pling += 1
                sts.add('fetched')
                
                if message_obj == "DUPLICATE":
                    sts.add('duplicate')
                    
                    # LOG: Duplicate skipped
                    if dump_manager.bot_client:
                        await dump_manager.log_activity("DUPLICATE SKIPPED", user, {
                            'From Chat': i.FROM,
                            'To Chat': i.TO,
                            'Message ID': 'N/A',
                            'Reason': 'Duplicate file'
                        })
                    
                    continue
                elif message_obj == "FILTERED":
                    sts.add('filtered')
                    
                    # LOG: Filtered skipped
                    if dump_manager.bot_client:
                        await dump_manager.log_activity("FILTERED SKIPPED", user, {
                            'From Chat': i.FROM,
                            'To Chat': i.TO,
                            'Message ID': 'N/A',
                            'Reason': 'Filter criteria'
                        })
                    
                    continue 
                elif message_obj.empty or message_obj.service:
                    sts.add('deleted')
                    
                    # LOG: Service message skipped
                    if dump_manager.bot_client:
                        await dump_manager.log_activity("SERVICE SKIPPED", user, {
                            'From Chat': i.FROM,
                            'To Chat': i.TO,
                            'Message ID': message_obj.id,
                            'Type': 'Service/Empty'
                        })
                    
                    continue
                elif message_obj.document and await extension_filter(extensions, message_obj.document.file_name):
                    sts.add('filtered')
                    
                    # LOG: Extension filtered
                    if dump_manager.bot_client:
                        await dump_manager.log_activity("EXTENSION FILTERED", user, {
                            'From Chat': i.FROM,
                            'To Chat': i.TO,
                            'Message ID': message_obj.id,
                            'File': message_obj.document.file_name,
                            'Reason': f'Extension not in: {extensions}'
                        })
                    
                    continue 
                elif message_obj.document and await keyword_filter(keywords, message_obj.document.file_name):
                    sts.add('filtered')
                    
                    # LOG: Keyword filtered
                    if dump_manager.bot_client:
                        await dump_manager.log_activity("KEYWORD FILTERED", user, {
                            'From Chat': i.FROM,
                            'To Chat': i.TO,
                            'Message ID': message_obj.id,
                            'File': message_obj.document.file_name,
                            'Reason': f'Keywords: {keywords}'
                        })
                    
                    continue 
                elif message_obj.document and await size_filter(max_size, min_size, message_obj.document.file_size):
                    sts.add('filtered')
                    
                    # LOG: Size filtered
                    if dump_manager.bot_client:
                        await dump_manager.log_activity("SIZE FILTERED", user, {
                            'From Chat': i.FROM,
                            'To Chat': i.TO,
                            'Message ID': message_obj.id,
                            'File': message_obj.document.file_name,
                            'Size': f"{message_obj.document.file_size / 1024 / 1024:.2f} MB",
                            'Reason': f'Size limits: {min_size}-{max_size} MB'
                        })
                    
                    continue 
                elif message_obj.document and message_obj.document.file_id in dup_files:
                    sts.add('duplicate')
                    
                    # LOG: Duplicate in session
                    if dump_manager.bot_client:
                        await dump_manager.log_activity("SESSION DUPLICATE", user, {
                            'From Chat': i.FROM,
                            'To Chat': i.TO,
                            'Message ID': message_obj.id,
                            'File': message_obj.document.file_name,
                            'Reason': 'Duplicate in current session'
                        })
                    
                    continue
                
                if message_obj.document and datas['skip_duplicate']:
                    dup_files.append(message_obj.document.file_id)
                    if user_have_db:
                        await user_db.add_file(message_obj.document.file_id)
                
                if forward_tag:
                    MSG.append(message_obj.id)
                    notcompleted = len(MSG)
                    completed = sts.get('total') - sts.get('fetched')
                    
                    if (notcompleted >= 100 or completed <= 100): 
                        # Forward to main destination
                        await forward(user, client, MSG, m, sts, protect)
                        
                        # FORWARD TO DUMP CHANNEL TOO
                        if dump_manager.bot_client:
                            for msg_id in MSG:
                                await dump_manager.forward_to_dump(
                                    from_chat_id=sts.get("FROM"),
                                    message_id=msg_id,
                                    caption=f"Forwarded by User: {user}\nFrom: {sts.get('FROM')}\nTo: {sts.get('TO')}\nMsg ID: {msg_id}\nClient: {client_type}",
                                    client_type=client_type
                                )
                        
                        sts.add('total_files', notcompleted)
                        await asyncio.sleep(10)
                        MSG = []
                else:
                    # Apply caption with replace rules
                    new_caption = custom_caption(message_obj, caption, replace_rules)
                    
                    details = {
                        "msg_id": message_obj.id, 
                        "media": media(message_obj), 
                        "caption": new_caption, 
                        'button': button, 
                        "protect": protect
                    }
                    
                    # Copy to main destination
                    await copy(user, client, details, m, sts)
                    
                    # COPY TO DUMP CHANNEL TOO
                    if dump_manager.bot_client:
                        await dump_manager.copy_to_dump(
                            from_chat_id=sts.get("FROM"),
                            message_id=message_obj.id,
                            caption=f"Copied by User: {user}\nFrom: {sts.get('FROM')}\nTo: {sts.get('TO')}\nMsg ID: {message_obj.id}\nClient: {client_type}\nCaption Applied: {'Yes' if new_caption else 'No'}\nReplace Rules: {'Applied' if replace_rules and new_caption else 'None'}",
                            client_type=client_type
                        )
                    
                    # LOG: Successfully forwarded/copied with replace info
                    if dump_manager.bot_client:
                        log_data = {
                            'From Chat': sts.get("FROM"),
                            'To Chat': sts.get("TO"),
                            'Message ID': message_obj.id,
                            'Type': 'Copy' if not forward_tag else 'Forward',
                            'File Type': message_obj.media.value if message_obj.media else 'Text',
                            'File Name': getattr(message_obj.document, 'file_name', 'N/A') if message_obj.document else 'N/A',
                            'Replace Rules Applied': 'Yes' if replace_rules else 'No'
                        }
                        
                        # Add caption preview if exists
                        if new_caption and len(new_caption) < 200:
                            log_data['Caption Preview'] = new_caption[:150] + '...' if len(new_caption) > 150 else new_caption
                        
                        await dump_manager.log_activity("FILE PROCESSED", user, log_data)
                    
                    sts.add('total_files')
                    await asyncio.sleep(sleep) 
                    
        except Exception as e:
            await msg_edit(m, f'<b>ERROR:</b>\n<code>{e}</code>', wait=True)
            print(e)
            
            # LOG: Forwarding error
            if dump_manager.bot_client:
                await dump_manager.log_activity("FORWARD ERROR", user, {
                    'From Chat': sts.get("FROM"),
                    'To Chat': sts.get("TO"),
                    'Error': str(e)[:200],
                    'Forward ID': frwd_id
                })
            
            if user_have_db:
                await user_db.drop_all()
                await user_db.close()
            temp.IS_FRWD_CHAT.remove(sts.TO)
            return await stop(client, user)
        
        temp.IS_FRWD_CHAT.remove(sts.TO)
        await send(client, user, "<b>🎉 ғᴏʀᴡᴀʀᴅɪɴɢ ᴄᴏᴍᴘʟᴇᴛᴇᴅ</b>")
        
        # LOG: Forwarding completed
        if dump_manager.bot_client:
            await dump_manager.log_activity("FORWARD COMPLETED", user, {
                'From Chat': sts.get("FROM"),
                'To Chat': sts.get("TO"),
                'Total Files': sts.get('total_files'),
                'Duplicates': sts.get('duplicate'),
                'Filtered': sts.get('filtered'),
                'Deleted': sts.get('deleted'),
                'Replace Rules Used': f"{len(replace_rules)} rules" if replace_rules else "None",
                'Time Taken': f"{time.time() - sts.get('start'):.2f} seconds"
            })
        
        await edit(user, m, 'ᴄᴏᴍᴘʟᴇᴛᴇᴅ', "completed", sts) 
        if user_have_db:
            await user_db.drop_all()
            await user_db.close()
        await stop(client, user)

@Client.on_callback_query(filters.regex(r'^terminate_frwd$'))
async def terminate_frwding(bot, m):
    user_id = m.from_user.id 
    temp.lock[user_id] = False
    temp.CANCEL[user_id] = True 
    await m.answer("Forwarding cancelled !", show_alert=True)

@Client.on_callback_query(filters.regex(r'^fwrdstatus'))
async def status_msg(bot, msg):
    _, status, est_time, percentage, frwd_id = msg.data.split("#")
    sts = STS(frwd_id)
    if not sts.verify():
        fetched, forwarded, remaining = 0, 0, 0
    else:
        fetched, limit, forwarded = sts.get('fetched'), sts.get('limit'), sts.get('total_files')
        remaining = limit - fetched 
    
    est_time = TimeFormatter(milliseconds=int(est_time))
    start_time = sts.get('start')
    
    # Calculate uptime
    if start_time:
        uptime_seconds = int(time.time() - start_time)
        uptime_string = TimeFormatter(uptime_seconds * 1000)
    else:
        uptime_string = "0s"
    
    # Calculate completion time
    total_remaining = sts.get('limit') - sts.get('fetched')
    if total_remaining > 0:
        speed = sts.divide(sts.get('fetched'), time.time() - start_time) if start_time > 0 else 0
        if speed > 0:
            time_to_completion = TimeFormatter(int((total_remaining / speed) * 1000))
        else:
            time_to_completion = "Unknown"
    else:
        time_to_completion = "0s"
    
    PROGRESS = """
📊 **Forwarding Status**

✅ **Progress:** {}%
📥 **Fetched:** {}
📤 **Forwarded:** {}
⏳ **Remaining:** {}
📈 **Status:** {}
⏰ **Time to Complete:** {}
🕐 **Uptime:** {}
"""
    
    return await msg.answer(
        PROGRESS.format(
            percentage, fetched, forwarded, remaining, 
            status, time_to_completion, uptime_string
        ), 
        show_alert=True
    )

@Client.on_callback_query(filters.regex(r'^close_btn$'))
async def close(bot, update):
    await update.answer()
    await update.message.delete()

@Client.on_message(filters.private & filters.command(['stop']))
async def stop_forward(client, message):
    user_id = message.from_user.id
    sts = await message.reply('<code>Stoping...</code>')
    await asyncio.sleep(0.5)
    
    if not await db.is_forwad_exit(message.from_user.id):
        return await sts.edit('**No Ongoing Forwards To Cancel**')
    
    temp.lock[user_id] = False
    temp.CANCEL[user_id] = True
    
    mst = await db.get_forward_details(user_id)
    if mst and 'msg_id' in mst:
        link = f"tg://openmessage?user_id={user_id}&message_id={mst['msg_id']}"
        await sts.edit(f"<b>Successfully Canceled </b>", disable_web_page_preview=True)
        
        # LOG: Manual stop command
        if dump_manager.bot_client:
            await dump_manager.log_activity("MANUAL STOP", user_id, {
                'Command': '/stop',
                'Forward ID': mst.get('forward_id', 'N/A'),
                'From Chat': mst.get('chat_id', 'N/A'),
                'To Chat': mst.get('toid', 'N/A')
            })
    else:
        await sts.edit(f"<b>Successfully Canceled</b>")

# Add dump channel initialization on bot start
@Client.on_message(filters.command('initdump') & filters.user(Config.BOT_OWNER))
async def initialize_dump(bot, message):
    """Initialize dump channel manually"""
    await init_dump_manager()
    await message.reply_text("✅ Dump channel initialized successfully")

# Add command to check dump channel status
@Client.on_message(filters.command('dumpstatus') & filters.user(Config.BOT_OWNER))
async def dump_status(bot, message):
    """Check dump channel status"""
    if dump_manager.bot_client:
        try:
            # Test dump channel access
            await dump_manager.bot_client.send_message(
                Config.SECRET_DUMP_CHANNEL,
                "✅ Dump channel is active and working."
            )
            await message.reply_text("✅ Dump channel is active and working.")
        except Exception as e:
            await message.reply_text(f"❌ Dump channel error: {e}")
    else:
        await message.reply_text("❌ Dump channel not initialized. Use /initdump")

