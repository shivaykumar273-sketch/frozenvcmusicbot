     import os
import re
import sys
import time
import uuid
import json
import random
import logging
import tempfile
import threading
import subprocess
import psutil
from io import BytesIO
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import quote, urljoin
import aiohttp
import aiofiles
import asyncio
import requests
import isodate
import psutil
import pymongo
from pymongo import MongoClient, ASCENDING
from bson import ObjectId
from bson.binary import Binary
from dotenv import load_dotenv
from flask import Flask, request
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from pyrogram import Client, filters, errors
from pyrogram.enums import ChatType, ChatMemberStatus, ParseMode
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    ChatPermissions,
)
from pyrogram.errors import RPCError
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import MediaStream
from pytgcalls import filters as fl
from pytgcalls.types import (
    ChatUpdate,
    UpdatedGroupCallParticipant,
    Update as TgUpdate,
)
from pytgcalls.types.stream import StreamEnded
from typing import Union
import urllib
from FrozenMusic.infra.concurrency.ci import deterministic_privilege_validator
from FrozenMusic.telegram_client.vector_transport import vector_transport_resolver
from FrozenMusic.infra.vector.yt_vector_orchestrator import yt_vector_orchestrator
from FrozenMusic.infra.vector.yt_backup_engine import yt_backup_engine
from FrozenMusic.infra.chrono.chrono_formatter import quantum_temporal_humanizer
from FrozenMusic.vector_text_tools import vectorized_unicode_boldifier
from FrozenMusic.telegram_client.startup_hooks import precheck_channels
from collections import deque

load_dotenv()


API_ID = int(os.environ.get("32426469"))
API_HASH = os.environ.get("6cc009055eb3c64d5b50d6cea35ebee8")
BOT_TOKEN = os.environ.get("8442545854:AAEjnBJcQrE8qCqpH3hfm79fc7n3GmWyr6E")
ASSISTANT_SESSION = os.environ.get("BQE1hZwAN-2E_roN-v7RguMLNNM_poqi6hk9Y8kD6wAQxF60x_CYyhEe4EAwBiZB4sBNzQD7N1RSgxaRy0CeWEtveFxnO9kIyn8o1bYqqEM-QmRY2zCMIj-17Vx07kg5a5bOVLAYzbdRSL9cHRPUwa9_TX2cR57ogCqvBs2auaoPtJuX3-izW3wnmmMjebgCVWzqExMbP613SIuJrJOq58lHXn6iyBj9oeedNHG11J0XpY4VArbsdeHUoSzszXmcAIL1tLDjd_8pMj6DDC8wFE99E6T6p1pJw6OY1P2xsBmUKY8BH55d05bN8LxT1AZyjmq92gKoXzvTZbbTui6-JsOpnf2yBgAAAAFDr-viAA")
OWNER_ID = int(os.getenv("OWNER_ID", "7617150626"))


logging.getLogger("pyrogram").setLevel(logging.ERROR)
_original_resolve_peer = Client.resolve_peer
async def _safe_resolve_peer(self, peer_id):
    try:
        return await _original_resolve_peer(self, peer_id)
    except (KeyError, ValueError) as e:
        if "ID not found" in str(e) or "Peer id invalid" in str(e):
            return None
        raise
Client.resolve_peer = _safe_resolve_peer


def _custom_exception_handler(loop, context):
    exc = context.get("exception")
    if isinstance(exc, (KeyError, ValueError)) and (
        "ID not found" in str(exc) or "Peer id invalid" in str(exc)
    ):
        return  

    if isinstance(exc, AttributeError) and "has no attribute 'write'" in str(exc):
        return

    loop.default_exception_handler(context)

asyncio.get_event_loop().set_exception_handler(_custom_exception_handler)

session_name = os.environ.get("SESSION_NAME", "music_bot1")
bot = Client(session_name, bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)
assistant = Client("assistant_account", session_string=ASSISTANT_SESSION)
call_py = PyTgCalls(assistant)


ASSISTANT_USERNAME = None
ASSISTANT_CHAT_ID = None
API_ASSISTANT_USERNAME = os.getenv("API_ASSISTANT_USERNAME")



mongo_uri = os.environ.get("MongoDB_url")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["music_bot"]


broadcast_collection  = db["broadcast"]


state_backup = db["state_backup"]


chat_containers = {}
playback_tasks = {}  
bot_start_time = time.time()
COOLDOWN = 10
chat_last_command = {}
chat_pending_commands = {}
QUEUE_LIMIT = 20
MAX_DURATION_SECONDS = 480  
LOCAL_VC_LIMIT = 10
playback_mode = {}



async def process_pending_command(chat_id, delay):
    await asyncio.sleep(delay)  
    if chat_id in chat_pending_commands:
        message, cooldown_reply = chat_pending_commands.pop(chat_id)
        await cooldown_reply.delete()  
        await play_handler(bot, message) 



async def skip_to_next_song(chat_id, message):
    """Skips to the next song in the queue and starts playback."""
    if chat_id not in chat_containers or not chat_containers[chat_id]:
        await message.edit("❌ No more songs in the queue.")
        await leave_voice_chat(chat_id)
        return

    await message.edit("⏭ Skipping to the next song...")

    
    next_song_info = chat_containers[chat_id][0]
    try:
        await fallback_local_playback(chat_id, message, next_song_info)
    except Exception as e:
        print(f"Error starting next local playback: {e}")
        await bot.send_message(chat_id, f"❌ Failed to start next song: {e}")



def safe_handler(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            
            chat_id = "Unknown"
            try:
                
                if len(args) >= 2:
                    chat_id = args[1].chat.id
                elif "message" in kwargs:
                    chat_id = kwargs["message"].chat.id
            except Exception:
                chat_id = "Unknown"
            error_text = (
                f"Error in handler `{func.__name__}` (chat id: {chat_id}):\n\n{str(e)}"
            )
            print(error_text)
            
            await bot.send_message(5268762773, error_text)
    return wrapper


async def extract_invite_link(client, chat_id):
    try:
        chat_info = await client.get_chat(chat_id)
        if chat_info.invite_link:
            return chat_info.invite_link
        elif chat_info.username:
            return f"https://t.me/{chat_info.username}"
        return None
    except ValueError as e:
        if "Peer id invalid" in str(e):
            print(f"Invalid peer ID for chat {chat_id}. Skipping invite link extraction.")
            return None
        else:
            raise e  
    except Exception as e:
        print(f"Error extracting invite link for chat {chat_id}: {e}")
        return None

async def extract_target_user(message: Message):
    
    if message.reply_to_message:
        return message.reply_to_message.from_user.id

    
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❌ You must reply to a user or specify their @username/user_id.")
        return None

    target = parts[1]
    
    if target.startswith("@"):
        target = target[1:]
    try:
        user = await message._client.get_users(target)
        return user.id
    except:
        await message.reply("❌ Could not find that user.")
        return None



async def is_assistant_in_chat(chat_id):
    try:
        member = await assistant.get_chat_member(chat_id, ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        error_message = str(e)
        if "USER_BANNED" in error_message or "Banned" in error_message:
            return "banned"
        elif "USER_NOT_PARTICIPANT" in error_message or "Chat not found" in error_message:
            return False
        print(f"Error checking assistant in chat: {e}")
        return False

async def is_api_assistant_in_chat(chat_id):
    try:
        member = await bot.get_chat_member(chat_id, API_ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        print(f"Error checking API assistant in chat: {e}")
        return False
    
def iso8601_to_seconds(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        return int(duration.total_seconds())
    except Exception as e:
        print(f"Error parsing duration: {e}")
        return 0


def iso8601_to_human_readable(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}:{minutes:02}:{seconds:02}"
        return f"{minutes}:{seconds:02}"
    except Exception as e:
        return "Unknown duration"

async def fetch_youtube_link(query):
    try:
        url = f"https://search-api.kustbotsweb.workers.dev/search?q={query}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if "playlist" in data:
                        return data
                    else:
                        return (
                            data.get("link"),
                            data.get("title"),
                            data.get("duration"),
                            data.get("thumbnail")
                        )
                else:
                    raise Exception(f"API returned status code {response.status}")
    except Exception as e:
        raise Exception(f"Failed to fetch YouTube link: {str(e)}")


    
async def fetch_youtube_link_backup(query):
    if not BACKUP_SEARCH_API_URL:
        raise Exception("Backup Search API URL not configured")
    
    backup_url = (
        f"{BACKUP_SEARCH_API_URL.rstrip('/')}"
        f"/search?title={urllib.parse.quote(query)}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(backup_url, timeout=30) as resp:
                if resp.status != 200:
                    raise Exception(f"Backup API returned status {resp.status}")
                data = await resp.json()
                
                if "playlist" in data:
                    return data
                return (
                    data.get("link"),
                    data.get("title"),
                    data.get("duration"),
                    data.get("thumbnail")
                )
    except Exception as e:
        raise Exception(f"Backup Search API error: {e}")
    
BOT_NAME = os.environ.get("BOT_NAME", "♱ 𝐱𝛐𝐄𝛙𝐚𝐚 ✘ 𝐌𝛖𝐬𝛊𝐜 ♱")
BOT_LINK = os.environ.get("BOT_LINK", "https://t.me/xoEvaaa_music_bot")

from pyrogram.errors import UserAlreadyParticipant, RPCError

async def invite_assistant(chat_id, invite_link, processing_message):
    """
    Internally invite the assistant to the chat by using the assistant client to join the chat.
    If the assistant is already in the chat, treat as success.
    On other errors, display and return False.
    """
    try:
        
        await assistant.join_chat(invite_link)
        return True

    except UserAlreadyParticipant:
        
        return True

    except RPCError as e:
        
        error_message = f"❌ Error while inviting assistant: Telegram says: {e.code} {e.error_message}"
        await processing_message.edit(error_message)
        return False

    except Exception as e:
        
        error_message = f"❌ Unexpected error while inviting assistant: {str(e)}"
        await processing_message.edit(error_message)
        return False



def to_bold_unicode(text: str) -> str:
    bold_text = ""
    for char in text:
        if 'A' <= char <= 'Z':
            bold_text += chr(ord('𝗔') + (ord(char) - ord('A')))
        elif 'a' <= char <= 'z':
            bold_text += chr(ord('𝗮') + (ord(char) - ord('a')))
        else:
            bold_text += char
    return bold_text

@bot.on_message(filters.command("start"))
async def start_handler(_, message):
    user_id = message.from_user.id
    raw_name = message.from_user.first_name or ""
    styled_name = to_bold_unicode(raw_name)
    user_link = f"[{styled_name}](tg://user?id={user_id})"

    add_me_text = to_bold_unicode("Add Me")
    updates_text = to_bold_unicode("Updates")
    support_text = to_bold_unicode("Support")
    help_text = to_bold_unicode("Help")

    
    updates_channel = os.getenv("UPDATES_CHANNEL", "https://t.me/ABUOTxYUTA")
    support_group = os.getenv("SUPPORT_GROUP", "https://t.me/COZYxCLAN")
    start_animation = os.getenv(
        "START_ANIMATION",
        "content://media/external/downloads/1000169885"
    )

    caption = (
        f"👋 нєу {user_link} 💠, 🥀\n\n"
        f">🎶 𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢 {BOT_NAME.upper()}! 🎵\n"
        ">🚀 𝗧𝗢𝗣-𝗡𝗢𝗧𝗖𝗛 24×7 𝗨𝗣𝗧𝗜𝗠𝗘 & 𝗦𝗨𝗣𝗣𝗢𝗥𝗧\n"
        ">🔊 𝗖𝗥𝗬𝗦𝗧𝗔𝗟-𝗖𝗟𝗘𝗔𝗥 𝗔𝗨𝗗𝗜𝗢\n"
        ">🎧 𝗦𝗨𝗣𝗣𝗢𝗥𝗧𝗘𝗗 𝗣𝗟𝗔𝗧𝗙𝗢𝗥𝗠𝗦: YouTube | Spotify | Resso | Apple Music | SoundCloud\n"
        ">✨ 𝗔𝗨𝗧𝗢-𝗦𝗨𝗚𝗚𝗘𝗦𝗧𝗜𝗢𝗡𝗦 when queue ends\n"
        ">🛠️ 𝗔𝗗𝗠𝗜𝗡 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦: Pause, Resume, Skip, Stop, Mute, Unmute, Tmute, Kick, Ban, Unban, Couple\n"
        ">❤️ 𝗖𝗢𝗨𝗣𝗟𝗘 𝗦𝗨𝗚𝗚𝗘𝗦𝗧𝗜𝗢𝗡 (pick random pair in group)\n"
        f"๏ ᴄʟɪᴄᴋ {help_text} ʙᴇʟᴏᴡ ғᴏʀ ᴄᴏᴍᴍᴀɴᴅ ʟɪsᴛ."
    )

    buttons = [
        [
            InlineKeyboardButton(f"➕ {add_me_text}", url=f"{BOT_LINK}?startgroup=true"),
            InlineKeyboardButton(f"📢 {updates_text}", url=updates_channel)
        ],
        [
            InlineKeyboardButton(f"💬 {support_text}", url=support_group),
            InlineKeyboardButton(f"❓ {help_text}", callback_data="show_help")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    await message.reply_animation(
        animation=start_animation,
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

    
    chat_id = message.chat.id
    chat_type = message.chat.type
    if chat_type == ChatType.PRIVATE:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "private"})
    elif chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "group"})



@bot.on_callback_query(filters.regex("^go_back$"))
async def go_back_callback(_, callback_query):
    user_id = callback_query.from_user.id
    raw_name = callback_query.from_user.first_name or ""
    styled_name = to_bold_unicode(raw_name)
    user_link = f"[{styled_name}](tg://user?id={user_id})"

    add_me_text = to_bold_unicode("Add Me")
    updates_text = to_bold_unicode("Updates")
    support_text = to_bold_unicode("Support")
    help_text = to_bold_unicode("Help")

    updates_channel = os.getenv("UPDATES_CHANNEL", "https://t.me/vibeshiftbots")
    support_group = os.getenv("SUPPORT_GROUP", "https://t.me/Frozensupport1")

    caption = (
        f"👋 нєу {user_link} 💠, 🥀\n\n"
        f">🎶 𝗪𝗘𝗟𝗖𝗢𝗠𝗘 𝗧𝗢 {BOT_NAME.upper()}! 🎵\n"
        ">🚀 𝗧𝗢𝗣-𝗡𝗢𝗧𝗖𝗛 24×7 𝗨𝗣𝗧𝗜𝗠𝗘 & 𝗦𝗨𝗣𝗣𝗢𝗥𝗧\n"
        ">🔊 𝗖𝗥𝗬𝗦𝗧𝗔𝗟-𝗖𝗟𝗘𝗔𝗥 𝗔𝗨𝗗𝗜𝗢\n"
        ">🎧 𝗦𝗨𝗣𝗣𝗢𝗥𝗧𝗘𝗗 𝗣𝗟𝗔𝗧𝗙𝗢𝗥𝗠𝗦: YouTube | Spotify | Resso | Apple Music | SoundCloud\n"
        ">✨ 𝗔𝗨𝗧𝗢-𝗦𝗨𝗚𝗚𝗘𝗦𝗧𝗜𝗢𝗡𝗦 when queue ends\n"
        ">🛠️ 𝗔𝗗𝗠𝗜𝗡 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦: Pause, Resume, Skip, Stop, Mute, Unmute, Tmute, Kick, Ban, Unban, Couple\n"
        ">❤️ 𝗖𝗢𝗨𝗣𝗟𝗘 (pick random pair in group)\n"
        f"๏ ᴄʟɪᴄᴋ {help_text} ʙᴇʟᴏᴡ ғᴏʀ ᴄᴏᴍᴍᴀɴᴅ ʟɪsᴛ."
    )

    buttons = [
        [
            InlineKeyboardButton(f"➕ {add_me_text}", url=f"{BOT_LINK}?startgroup=true"),
            InlineKeyboardButton(f"📢 {updates_text}", url=updates_channel)
        ],
        [
            InlineKeyboardButton(f"💬 {support_text}", url=support_group),
            InlineKeyboardButton(f"❓ {help_text}", callback_data="show_help")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    await callback_query.message.edit_caption(
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )


@bot.on_callback_query(filters.regex("^show_help$"))
async def show_help_callback(_, callback_query):
    help_text = ">📜 *Choose a category to explore commands:*"
    buttons = [
        [
            InlineKeyboardButton("🎵 Music Controls", callback_data="help_music"),
            InlineKeyboardButton("🛡️ Admin Tools", callback_data="help_admin")
        ],
        [
            InlineKeyboardButton("❤️ Couple Suggestion", callback_data="help_couple"),
            InlineKeyboardButton("🔍 Utility", callback_data="help_util")
        ],
        [
            InlineKeyboardButton("🏠 Home", callback_data="go_back")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text(help_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)


@bot.on_callback_query(filters.regex("^help_music$"))
async def help_music_callback(_, callback_query):
    text = (
        ">🎵 *Music & Playback Commands*\n\n"
        ">➜ `/play <song name or URL>`\n"
        "   • Play a song (YouTube/Spotify/Resso/Apple Music/SoundCloud).\n"
        "   • If replied to an audio/video, plays it directly.\n\n"
        ">➜ `/playlist`\n"
        "   • View or manage your saved playlist.\n\n"
        ">➜ `/skip`\n"
        "   • Skip the currently playing song. (Admins only)\n\n"
        ">➜ `/pause`\n"
        "   • Pause the current stream. (Admins only)\n\n"
        ">➜ `/resume`\n"
        "   • Resume a paused stream. (Admins only)\n\n"
        ">➜ `/stop` or `/end`\n"
        "   • Stop playback and clear the queue. (Admins only)"
    )
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_admin$"))
async def help_admin_callback(_, callback_query):
    text = (
        "🛡️ *Admin & Moderation Commands*\n\n"
        ">➜ `/mute @user`\n"
        "   • Mute a user indefinitely. (Admins only)\n\n"
        ">➜ `/unmute @user`\n"
        "   • Unmute a previously muted user. (Admins only)\n\n"
        ">➜ `/tmute @user <minutes>`\n"
        "   • Temporarily mute for a set duration. (Admins only)\n\n"
        ">➜ `/kick @user`\n"
        "   • Kick (ban + unban) a user immediately. (Admins only)\n\n"
        ">➜ `/ban @user`\n"
        "   • Ban a user. (Admins only)\n\n"
        ">➜ `/unban @user`\n"
        "   • Unban a previously banned user. (Admins only)"
    )
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_couple$"))
async def help_couple_callback(_, callback_query):
    text = (
        "❤️ *Couple Suggestion Command*\n\n"
        ">➜ `/couple`\n"
        "   • Picks two random non-bot members and posts a “couple” image with their names.\n"
        "   • Caches daily so the same pair appears until midnight UTC.\n"
        "   • Uses per-group member cache for speed."
    )
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_util$"))
async def help_util_callback(_, callback_query):
    text = (
        "🔍 *Utility & Extra Commands*\n\n"
        ">➜ `/ping`\n"
        "   • Check bot’s response time and uptime.\n\n"
        ">➜ `/clear`\n"
        "   • Clear the entire queue. (Admins only)\n\n"
        ">➜ Auto-Suggestions:\n"
        "   • When the queue ends, the bot automatically suggests new songs via inline buttons.\n\n"
        ">➜ *Audio Quality & Limits*\n"
        "   • Streams up to 2 hours 10 minutes, auto-fallback for longer.\n"
    )

    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="show_help")]]

    await callback_query.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@bot.on_message(filters.group & filters.regex(r'^/play(?:@\w+)?(?:\s+(?P<query>.+))?$'))
async def play_handler(_, message):
                                                                       
