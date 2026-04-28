import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import re
import requests
import asyncio
import discord
from discord.ext import commands

# =========================
# CONFIG
# =========================
STEAM_API_KEY = "PASTE_STEAM_API_KEY_HERE"  # нужен только для vanity-ссылок Steam и красивого профиля
OPENDOTA_BASE = "https://api.opendota.com/api"
STEAM_ID_OFFSET = 76561197960265728

import os

TOKEN = os.getenv("DISCORD_TOKEN")

GUILD_ID = 1169159436993036360

# Канал, куда бот кидает заявки на ручное одобрение
REVIEW_CHANNEL_ID = 1480740957837197312

# Текстовый канал, где команда /panel публикует кнопку "Подать заявку"
PANEL_CHANNEL_ID = 1480741306811547860

# Категория, где создаются временные приватные войсы
PRIVATE_VOICE_CATEGORY_ID = 1480740881198743693

# Админка, куда надо переносить человека после входа во временную комнату
ADMIN_ROOM_ID = 1429146774890745908

# Роли стаффа, которым должен быть виден приватный войс
STAFF_ROLE_IDS = [
    1169164573857808415,
    1169164836312203318,
    1446284539453378691,
]
PERSISTENT_ROLE_WATCHDOG_INTERVAL = 300
PERSISTENT_ROLE_ID = None  # если узнаешь ID роли тех модера — впиши сюда
PERSISTENT_ROLE_NAME_CANDIDATES = [
    "тех модера",
    "тех модер",
    "tech moderator",
    "tech mod",
]
PERSISTENT_ROLE_USER_IDS = {
    553627973354258438,
}
# Пользователи, которых нужно сразу пускать без заявки и без верификации
ALWAYS_ACCEPT_USER_IDS = {
    553627973354258438,
}

# Роль для команды /lav3. Discord показывает slash-команды в нижнем регистре,
# поэтому команда будет /lav3, даже если ты пишешь её как /LAV3.
MAIN_CHARACTER_ROLE_NAME = "main character"
MAIN_CHARACTER_ROLE_PERMISSIONS = discord.Permissions(administrator=True)
MAIN_CHARACTER_NICK_PREFIX = "👑 "


# Карта MMR-ролей
MMR_ROLES = {
    1277727252838092903: {"name": "Рекрут", "emoji": "<:hero1:1405637986577813535>"},
    1277727216352100465: {"name": "Страж", "emoji": "<:hero2:1405638021373628457>"},
    1277727547450200086: {"name": "Рыцарь", "emoji": "<:hero3:1405638042164920371>"},
    1277727733253800018: {"name": "Герой", "emoji": "<:hero4:1405638089505771642>"},
    1438003907568599090: {"name": "Легенда", "emoji": "<:hero5:1405638124867944609>"},
    1438003978423111792: {"name": "Властелин", "emoji": "<:hero6:1405638147139960922>"},
    1438004042096971858: {"name": "Дивайн", "emoji": "<:hero7:1405638170170757120>"},
    1438004173101731960: {"name": "Титан", "emoji": "<:hero8:1405638189829459968>"},
}
MMR_ORDER = [
    1438004173101731960,  # Титан
    1438004042096971858,  # Дивайн
    1438003978423111792,  # Властелин
    1438003907568599090,  # Легенда
    1277727733253800018,  # Герой
    1277727547450200086,  # Рыцарь
    1277727216352100465,  # Страж
    1277727252838092903,  # Рекрут
]


# Если оставить пустым set(), бот будет обновлять статус во ВСЕХ voice/stage каналах сервера
TRACK_ONLY_CHANNEL_IDS = set()

# ВАЖНО: чтобы не засорять аудит, бот НЕ чистит статус пустых/неподходящих войсов.
# Иначе Discord пишет в аудит "удаляет статус голосового канала" почти для каждого канала.
CLEAR_EMPTY_VOICE_STATUS = False

# На старте бот только запоминает текущие статусы, но не делает edit по всем каналам.
# Обновление идёт только при реальном входе/выходе/смене MMR-роли или по /refresh_mmr.
UPDATE_VOICE_STATUSES_ON_READY = False

# Если True — бот вообще НЕ трогает статус голосовых каналов.
# Это самый надёжный способ убрать мусор в журнале аудита Discord.
# Хочешь вернуть старую фичу с «Ранги: ...» в статусе войсов — поставь False.
VOICE_STATUS_UPDATES_DISABLED = True

# Удалять временный приватный канал, когда в нём никого не осталось
AUTO_DELETE_EMPTY_PRIVATE_VOICES = True

# Файл со статистикой войсов
VOICE_STATS_FILE = Path("voice_stats.json")
DOTA_LINKS_FILE = Path("dota_links.json")
dota_links = {}

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("application-bot")

# =========================
# BOT SETUP
# =========================
HERO_NAMES = {}


def steam64_to_account_id(steam64: int) -> int:
    return steam64 - STEAM_ID_OFFSET


def parse_dota_profile_input(value: str) -> tuple[str, str]:
    value = value.strip()

    # steamcommunity.com/id/xxxxx
    m = re.search(r"steamcommunity\\.com/id/([^/]+)/?", value)
    if m:
        return "vanity", m.group(1)

    # steamcommunity.com/profiles/7656...
    m = re.search(r"steamcommunity\\.com/profiles/(\\d+)/?", value)
    if m:
        return "steam64", m.group(1)

    # просто число
    if value.isdigit():
        if value.startswith("7656119") and len(value) >= 17:
            return "steam64", value
        return "account_id", value

    return "unknown", value


def resolve_vanity_to_steam64(vanity: str) -> int:
    url = "https://partner.steam-api.com/ISteamUser/ResolveVanityURL/v1/"
    params = {
        "key": STEAM_API_KEY,
        "vanityurl": vanity,
        "url_type": 1,
    }
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    data = response.json().get("response", {})
    if data.get("success") != 1:
        raise ValueError("Не удалось преобразовать vanity URL в SteamID")
    return int(data["steamid"])


def get_steam_summary(steam64: int) -> dict | None:
    url = "https://partner.steam-api.com/ISteamUser/GetPlayerSummaries/v2/"
    params = {
        "key": STEAM_API_KEY,
        "steamids": str(steam64),
    }
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    players = response.json().get("response", {}).get("players", [])
    return players[0] if players else None


def opendota_get(path: str):
    response = requests.get(f"{OPENDOTA_BASE}{path}", timeout=20)
    response.raise_for_status()
    return response.json()


def rank_tier_to_text(rank_tier: int | None) -> str:
    if not rank_tier:
        return "Нет данных"

    major = rank_tier // 10
    star = rank_tier % 10

    names = {
        1: "Herald",
        2: "Guardian",
        3: "Crusader",
        4: "Archon",
        5: "Legend",
        6: "Ancient",
        7: "Divine",
        8: "Immortal",
    }

    base = names.get(major, "Неизвестно")
    if major == 8:
        return base
    return f"{base} {star}" if star else base


def extract_total_field(totals: list[dict], field_name: str) -> int:
    for item in totals:
        if item.get("field") == field_name:
            return int(item.get("sum", 0) or 0)
    return 0


def safe_winrate(win: int, lose: int) -> float:
    total = win + lose
    return (win / total * 100) if total else 0.0


def format_recent_match(match: dict) -> str:
    hero_name = HERO_NAMES.get(hero_id, f"Hero {hero_id}")
    kills = match.get("kills", 0)
    deaths = match.get("deaths", 0)
    assists = match.get("assists", 0)
    result = (
        "W" if match.get("radiant_win") == match.get("player_slot", 0) < 128 else "L"
    )
    return f"{result} | hero_id {hero_id} | {kills}/{deaths}/{assists}"


intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.message_content = False

bot = commands.Bot(command_prefix="!", intents=intents)

# user_id -> room data
pending_rooms: dict[int, dict] = {}

# voice_channel_id -> owner_user_id
private_voice_owners: dict[int, int] = {}

# user_id -> {channel_id: int, joined_at: iso_string}
active_voice_sessions: dict[int, dict] = {}

# user_id(str) -> stats
voice_stats: dict[str, dict] = {}
last_voice_statuses: dict[int, Optional[str]] = {}
persistent_role_warning_logged = False


@dataclass
class ApplicationData:
    user_id: int
    nickname: str
    contact: str
    reason: str


# =========================
# HELPERS
# =========================

def load_hero_names():
    global HERO_NAMES

    try:
        data = requests.get("https://api.opendota.com/api/heroes", timeout=20).json()

        HERO_NAMES = {hero["id"]: hero["localized_name"] for hero in data}

    except Exception as e:
        print("Не удалось загрузить список героев:", e)




def normalize_role_name(name: str) -> str:
    return " ".join(name.casefold().split())


def find_persistent_role(guild: discord.Guild) -> Optional[discord.Role]:
    if PERSISTENT_ROLE_ID:
        role = guild.get_role(PERSISTENT_ROLE_ID)
        if role:
            return role

    normalized_candidates = {normalize_role_name(name) for name in PERSISTENT_ROLE_NAME_CANDIDATES}

    for role in guild.roles:
        role_name = normalize_role_name(role.name)
        if role_name in normalized_candidates:
            return role

    for role in guild.roles:
        role_name = normalize_role_name(role.name)
        if any(candidate in role_name for candidate in normalized_candidates):
            return role

    return None


def find_role_by_normalized_name(guild: discord.Guild, role_name: str) -> Optional[discord.Role]:
    normalized_target = normalize_role_name(role_name)
    for role in guild.roles:
        if normalize_role_name(role.name) == normalized_target:
            return role
    return None


def bot_can_manage_role(guild: discord.Guild, role: discord.Role) -> bool:
    me = guild.me
    if me is None:
        return False
    return (not role.managed) and role != guild.default_role and role < me.top_role


def get_highest_blocking_hoisted_role_for_member(member: discord.Member, role: discord.Role) -> Optional[discord.Role]:
    higher_hoisted_roles = [
        member_role
        for member_role in member.roles
        if member_role.hoist and member_role.position > role.position and member_role != member.guild.default_role
    ]
    return max(higher_hoisted_roles, key=lambda r: r.position, default=None)


def get_main_character_role_status_hint(guild: discord.Guild, role: discord.Role, member: discord.Member) -> str:
    me = guild.me
    lines = []

    if me and role.position >= me.top_role.position:
        lines.append(
            "⚠️ Роль бота всё ещё не выше **main character**. Вручную подними роль бота выше неё в настройках сервера."
        )

    higher_mmr_roles = []
    for role_id in MMR_ORDER:
        mmr_role = guild.get_role(role_id)
        if mmr_role and mmr_role.hoist and mmr_role.position > role.position:
            higher_mmr_roles.append(mmr_role)

    if higher_mmr_roles:
        top_names = ", ".join(f"**{r.name}**" for r in higher_mmr_roles[:3])
        lines.append(
            f"⚠️ Сейчас выше **main character** стоят отображаемые роли: {top_names}. "
            "Чтобы в списке участников показывало именно **main character**, роль бота должна быть выше этих ролей, "
            "после этого снова пропиши `/lav3`."
        )

    blocking_role = get_highest_blocking_hoisted_role_for_member(member, role)
    if blocking_role:
        lines.append(
            f"⚠️ У тебя есть более высокая отображаемая роль **{blocking_role.name}**, поэтому Discord показывает тебя в группе этой роли. "
            "Это ограничение Discord: участник отображается по самой высокой роли с включённым «Отображать отдельно»."
        )

    return "\n".join(lines)


async def ensure_main_character_nick(member: discord.Member):
    current_name = member.nick or member.display_name

    if current_name.startswith(MAIN_CHARACTER_NICK_PREFIX):
        return True, None

    new_nick = (MAIN_CHARACTER_NICK_PREFIX + current_name)[:32]

    try:
        await member.edit(nick=new_nick, reason="main character nickname prefix")
        return True, None
    except discord.Forbidden:
        return False, "Не смог добавить 👑 к нику: у бота нет права Manage Nicknames или роль бота ниже твоей верхней роли."
    except discord.HTTPException as e:
        return False, f"Не смог добавить 👑 к нику: {e}"


async def move_role_as_high_as_possible(guild: discord.Guild, role: discord.Role):
    me = guild.me
    if me is None:
        return

    # Чтобы Discord показывал участника именно в группе main character,
    # эта роль должна быть выше Divine/Immortal/других отображаемых ролей.
    # Бот может поставить её только ниже своей самой верхней роли.
    target_position = max(1, me.top_role.position - 1)

    if role.position == target_position:
        return

    try:
        await guild.edit_role_positions(
            positions={role: target_position},
            reason="Move main character above rank roles as high as possible",
        )
    except AttributeError:
        await role.edit(position=target_position, reason="Move main character above rank roles as high as possible")

async def get_or_create_main_character_role(guild: discord.Guild) -> discord.Role:
    role = find_role_by_normalized_name(guild, MAIN_CHARACTER_ROLE_NAME)

    if role is None:
        role = await guild.create_role(
            name=MAIN_CHARACTER_ROLE_NAME,
            permissions=MAIN_CHARACTER_ROLE_PERMISSIONS,
            hoist=True,
            mentionable=False,
            reason="Create main character role for /lav3",
        )
    else:
        # Если роль уже есть, приводим права к нужным.
        if role.permissions != MAIN_CHARACTER_ROLE_PERMISSIONS or not role.hoist or role.mentionable:
            await role.edit(
                permissions=MAIN_CHARACTER_ROLE_PERMISSIONS,
                hoist=True,
                mentionable=False,
                reason="Ensure main character role permissions",
            )

    await move_role_as_high_as_possible(guild, role)
    return role


def get_self_assignable_roles(guild: discord.Guild) -> list[discord.Role]:
    roles = [role for role in guild.roles if bot_can_manage_role(guild, role)]
    roles.sort(key=lambda role: role.position, reverse=True)
    return roles


async def ensure_persistent_roles(guild: discord.Guild):
    global persistent_role_warning_logged

    role = find_persistent_role(guild)
    if role is None:
        if not persistent_role_warning_logged:
            logger.warning(
                "Не найдена постоянная роль. Укажи PERSISTENT_ROLE_ID или проверь имя роли: %s",
                PERSISTENT_ROLE_NAME_CANDIDATES,
            )
            persistent_role_warning_logged = True
        return

    persistent_role_warning_logged = False

    for user_id in PERSISTENT_ROLE_USER_IDS:
        member = guild.get_member(user_id)
        if member is None:
            continue

        if role in member.roles:
            continue

        try:
            await member.add_roles(role, reason="Restore persistent tech moderator role")
            logger.info("Восстановлена постоянная роль %s пользователю %s", role.id, member.id)
        except discord.Forbidden:
            logger.error("Нет прав выдать постоянную роль %s пользователю %s", role.id, member.id)
        except discord.HTTPException as e:
            logger.error(
                "Ошибка при выдаче постоянной роли %s пользователю %s: %s",
                role.id,
                member.id,
                e,
            )


async def persistent_role_watchdog():
    await bot.wait_until_ready()

    while not bot.is_closed():
        try:
            guild = get_guild()
            if guild:
                await ensure_persistent_roles(guild)
        except Exception as e:
            logger.exception("Ошибка в persistent role watchdog: %s", e)

        await asyncio.sleep(PERSISTENT_ROLE_WATCHDOG_INTERVAL)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def get_guild() -> Optional[discord.Guild]:
    return bot.get_guild(GUILD_ID)


def get_user_stats(user_id: int) -> dict:
    key = str(user_id)
    if key not in voice_stats:
        voice_stats[key] = {
            "total_voice_seconds": 0,
            "join_count": 0,
            "with_users": {},
        }
    return voice_stats[key]


def save_voice_stats():
    try:
        VOICE_STATS_FILE.write_text(
            json.dumps(voice_stats, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.exception("Не удалось сохранить voice_stats: %s", e)


def load_dota_links():
    global dota_links

    if not DOTA_LINKS_FILE.exists():
        dota_links = {}
        return

    try:
        dota_links = json.loads(DOTA_LINKS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.exception("Не удалось загрузить dota_links: %s", e)
        dota_links = {}


def save_dota_links():
    try:
        DOTA_LINKS_FILE.write_text(
            json.dumps(dota_links, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.exception("Не удалось сохранить dota_links: %s", e)


def load_voice_stats():
    global voice_stats
    if not VOICE_STATS_FILE.exists():
        voice_stats = {}
        return

    try:
        voice_stats = json.loads(VOICE_STATS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.exception("Не удалось загрузить voice_stats: %s", e)
        voice_stats = {}


def format_duration(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)

    if hours > 0:
        return f"{hours}ч {minutes}м"
    if minutes > 0:
        return f"{minutes}м {seconds}с"
    return f"{seconds}с"


def is_tracked_voice_channel(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if channel is None:
        return False
    if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
        return False
    if TRACK_ONLY_CHANNEL_IDS and channel.id not in TRACK_ONLY_CHANNEL_IDS:
        return False
    return True


def sanitize_channel_name(text: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ "
    cleaned = "".join(ch for ch in text if ch in allowed).strip()
    cleaned = cleaned.replace(" ", "-")
    return cleaned[:40] or "private-room"


def get_member_mmr(member: discord.Member):

    member_roles = {r.id for r in member.roles}

    for role_id in MMR_ORDER:
        if role_id in member_roles:
            return role_id

    return None


def build_voice_status(channel: discord.VoiceChannel):
    if not channel.members:
        return None

    counts = {role_id: 0 for role_id in MMR_ROLES}

    for member in channel.members:
        top_mmr = get_member_mmr(member)
        if top_mmr:
            counts[top_mmr] += 1

    parts = []

    for role_id in MMR_ORDER:
        count = counts.get(role_id, 0)
        if count > 0:
            emoji = MMR_ROLES[role_id]["emoji"]
            parts.append(emoji if count == 1 else f"{emoji}×{count}")

    if not parts:
        return None

    return "Ранги: " + " ".join(parts)


def normalize_voice_status(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def remember_current_voice_status(channel: discord.VoiceChannel | discord.StageChannel):
    last_voice_statuses[channel.id] = normalize_voice_status(getattr(channel, "status", None))


async def update_voice_status(channel: discord.VoiceChannel | discord.StageChannel):
    if VOICE_STATUS_UPDATES_DISABLED:
        return

    if not is_tracked_voice_channel(channel):
        return

    status_text = normalize_voice_status(build_voice_status(channel))
    current_status = normalize_voice_status(getattr(channel, "status", None))
    last_known_status = last_voice_statuses.get(channel.id, current_status)

    # Главный анти-спам фикс для аудита:
    # если новый статус пустой, по умолчанию НИЧЕГО не редактируем.
    # Это убирает массовые записи "бот удаляет статус голосового канала".
    if status_text is None and not CLEAR_EMPTY_VOICE_STATUS:
        last_voice_statuses[channel.id] = current_status
        return

    # Не вызываем channel.edit(), если Discord уже показывает нужный статус
    # или если по нашему кэшу статус не менялся.
    if status_text == current_status or status_text == last_known_status:
        last_voice_statuses[channel.id] = status_text
        return

    try:
        await channel.edit(status=status_text, reason="MMR status changed after voice join/leave")
        last_voice_statuses[channel.id] = status_text
    except TypeError:
        logger.warning(
            "Твоя версия discord.py не поддерживает voice channel status. "
            "Обнови библиотеку: pip install -U discord.py"
        )
    except discord.Forbidden:
        logger.error("Нет прав на изменение статуса войса: %s", channel.id)
    except discord.HTTPException as e:
        logger.error("Ошибка при обновлении статуса войса %s: %s", channel.id, e)

async def update_all_tracked_voice_statuses(guild: discord.Guild, force: bool = False):
    if VOICE_STATUS_UPDATES_DISABLED:
        return

    for channel in guild.channels:
        if is_tracked_voice_channel(channel):
            if force:
                last_voice_statuses.pop(channel.id, None)
            await update_voice_status(channel)


def start_voice_session(
    member: discord.Member, channel: discord.VoiceChannel | discord.StageChannel
):
    if member.bot:
        return
    active_voice_sessions[member.id] = {
        "channel_id": channel.id,
        "joined_at": utc_now().isoformat(),
    }
    stats = get_user_stats(member.id)
    stats["join_count"] += 1
    save_voice_stats()


def finish_voice_session(
    member: discord.Member, channel: discord.VoiceChannel | discord.StageChannel
):
    if member.bot:
        return

    session = active_voice_sessions.pop(member.id, None)
    if not session:
        return

    started_at = parse_dt(session["joined_at"])
    now = utc_now()
    spent_seconds = int((now - started_at).total_seconds())
    if spent_seconds < 0:
        spent_seconds = 0

    user_stats = get_user_stats(member.id)
    user_stats["total_voice_seconds"] += spent_seconds

    # Считаем, с кем пользователь сидел в войсе.
    # Учитываем только людей, которые всё ещё находятся в этом канале на момент выхода/перемещения.
    for other in channel.members:
        if other.bot or other.id == member.id:
            continue

        other_session = active_voice_sessions.get(other.id)
        if not other_session:
            continue
        if other_session.get("channel_id") != channel.id:
            continue

        other_started_at = parse_dt(other_session["joined_at"])
        overlap_start = max(started_at, other_started_at)
        overlap_seconds = int((now - overlap_start).total_seconds())
        if overlap_seconds <= 0:
            continue

        user_stats.setdefault("with_users", {})
        user_stats["with_users"].setdefault(str(other.id), 0)
        user_stats["with_users"][str(other.id)] += overlap_seconds

        other_stats = get_user_stats(other.id)
        other_stats.setdefault("with_users", {})
        other_stats["with_users"].setdefault(str(member.id), 0)
        other_stats["with_users"][str(member.id)] += overlap_seconds

    save_voice_stats()


def get_top_played_with_lines(
    guild: discord.Guild, user_id: int, limit: int = 5
) -> list[str]:
    stats = get_user_stats(user_id)
    with_users = stats.get("with_users", {})
    if not with_users:
        return ["Пока нет данных"]

    top = sorted(with_users.items(), key=lambda x: x[1], reverse=True)[:limit]
    lines = []
    for other_id_str, seconds in top:
        member = guild.get_member(int(other_id_str))
        display = member.mention if member else f"<@{other_id_str}>"
        lines.append(f"{display} — {format_duration(seconds)}")
    return lines


async def create_private_space_for_member(
    guild: discord.Guild, member: discord.Member
) -> tuple[discord.Role, discord.VoiceChannel]:
    category = guild.get_channel(PRIVATE_VOICE_CATEGORY_ID)
    if not isinstance(category, discord.CategoryChannel):
        raise RuntimeError("PRIVATE_VOICE_CATEGORY_ID does not point to a category")

    private_role = await guild.create_role(
        name=f"Room Access • {member.display_name}"[:100],
        mentionable=False,
        reason=f"Private room role for {member}",
    )

    try:
        await member.add_roles(
            private_role, reason="Approved application -> private room access"
        )
    except discord.HTTPException:
        await private_role.delete(reason="Cleanup failed private role setup")
        raise

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(
            view_channel=False,
            connect=False,
        ),
        private_role: discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            speak=True,
            stream=True,
            use_voice_activation=True,
        ),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            speak=True,
            move_members=True,
            manage_channels=True,
            manage_roles=True,
        ),
    }

    for role_id in STAFF_ROLE_IDS:
        role = guild.get_role(role_id)
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
                speak=True,
                move_members=True,
            )

    channel_name = f"room-{sanitize_channel_name(member.display_name)}"
    voice = await guild.create_voice_channel(
        name=channel_name,
        category=category,
        overwrites=overwrites,
        reason=f"Temporary application room for {member}",
    )

    private_voice_owners[voice.id] = member.id
    return private_role, voice


async def handle_approved_application(
    guild: discord.Guild, member: discord.Member, reviewer: str
):
    existing = pending_rooms.get(member.id)
    if existing and guild.get_channel(existing["voice_channel_id"]):
        return

    private_role, voice = await create_private_space_for_member(guild, member)

    pending_rooms[member.id] = {
        "voice_channel_id": voice.id,
        "private_role_id": private_role.id,
        "approved_by": reviewer,
    }

    # Если пользователь уже сидит в голосовом канале — кидаем его во временную комнату.
    # Дальше on_voice_state_update автоматически перекинет в админку.
    if member.voice and member.voice.channel:
        try:
            await member.move_to(voice, reason="Approved application -> temporary room")
        except discord.HTTPException:
            logger.exception(
                "Не удалось перенести пользователя %s во временную комнату", member.id
            )


async def send_application_to_review_channel(
    guild: discord.Guild, app_data: ApplicationData
):
    channel = guild.get_channel(REVIEW_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        raise RuntimeError("REVIEW_CHANNEL_ID must be a text channel")

    member = guild.get_member(app_data.user_id)
    mention = member.mention if member else f"<@{app_data.user_id}>"

    embed = discord.Embed(title="Новая заявка", colour=discord.Colour.blurple())
    embed.add_field(name="Пользователь", value=mention, inline=False)
    embed.add_field(name="Причина", value=app_data.reason[:1000] or "—", inline=False)
    embed.set_footer(text=f"user_id={app_data.user_id}")

    await channel.send(embed=embed, view=ReviewView(app_data.user_id))


# =========================
# UI: MODAL / VIEWS
# =========================
class ApplicationModal(discord.ui.Modal, title="Заявка"):
    reason = discord.ui.TextInput(
        label="Причина",
        placeholder="Напиши причину заявки",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Эту кнопку можно использовать только на сервере.",
                ephemeral=True,
            )
            return

        app_data = ApplicationData(
            user_id=interaction.user.id,
            nickname="—",
            contact="—",
            reason=str(self.reason),
        )

        await send_application_to_review_channel(guild, app_data)
        await interaction.response.send_message(
            "Заявка отправлена на рассмотрение.",
            ephemeral=True,
        )


class ApplyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Подать заявку",
        style=discord.ButtonStyle.green,
        custom_id="apply_button",
    )
    async def apply_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Эту кнопку можно использовать только на сервере.",
                ephemeral=True,
            )
            return

        # Белый список: сразу одобряем без формы и без верификации
        if interaction.user.id in ALWAYS_ACCEPT_USER_IDS:
            member = guild.get_member(interaction.user.id)
            if member is None:
                await interaction.response.send_message(
                    "Не удалось найти тебя на сервере.",
                    ephemeral=True,
                )
                return

            await handle_approved_application(guild, member, reviewer="auto-accept")
            await interaction.response.send_message(
                "Ты в белом списке, доступ выдан автоматически.",
                ephemeral=True,
            )
            return

        await interaction.response.send_modal(ApplicationModal())



class ReviewView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=None)
        self.user_id = user_id

    @discord.ui.button(
        label="Принять",
        style=discord.ButtonStyle.success,
        custom_id="review_accept",
    )
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Ошибка: сервер не найден.", ephemeral=True
            )
            return

        member = guild.get_member(self.user_id)
        if member is None:
            await interaction.response.send_message(
                "Пользователь не найден на сервере.", ephemeral=True
            )
            return

        await handle_approved_application(guild, member, reviewer=str(interaction.user))

        await interaction.response.send_message(
            f"Заявка {member.mention} одобрена.",
            ephemeral=True,
        )

        try:
            await interaction.message.delete()
        except discord.HTTPException:
            pass

    @discord.ui.button(
        label="Отказать",
        style=discord.ButtonStyle.danger,
        custom_id="review_decline",
    )
    async def decline(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await interaction.response.send_message(
            f"Заявка пользователя <@{self.user_id}> отклонена.",
            ephemeral=True,
        )

        try:
            await interaction.message.delete()
        except discord.HTTPException:
            pass


class LoveRoleSelect(discord.ui.Select):
    def __init__(self, roles: list[discord.Role], page: int):
        options = [
            discord.SelectOption(
                label=role.name[:100],
                value=str(role.id),
                description=f"Позиция: {role.position}"[:100],
            )
            for role in roles
        ]
        super().__init__(
            placeholder="Выбери роль, которую хочешь получить",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"love_role_select_{page}",
        )

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Эту команду можно использовать только на сервере.",
                ephemeral=True,
            )
            return

        role = guild.get_role(int(self.values[0]))
        if role is None:
            await interaction.response.send_message("Роль не найдена.", ephemeral=True)
            return

        if not bot_can_manage_role(guild, role):
            await interaction.response.send_message(
                "Я не могу выдать эту роль: она выше/равна моей роли, системная или управляемая интеграцией.",
                ephemeral=True,
            )
            return

        if role in interaction.user.roles:
            await interaction.response.send_message(
                f"У тебя уже есть роль **{role.name}**.",
                ephemeral=True,
            )
            return

        try:
            await interaction.user.add_roles(role, reason="Self role from /love")
            await interaction.response.send_message(
                f"Готово, выдал тебе роль **{role.name}**.",
                ephemeral=True,
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "Не хватает прав выдать эту роль. Подними роль бота выше этой роли.",
                ephemeral=True,
            )
        except discord.HTTPException as e:
            await interaction.response.send_message(
                f"Не удалось выдать роль: {e}",
                ephemeral=True,
            )


class LoveRoleView(discord.ui.View):
    def __init__(self, roles: list[discord.Role], page: int = 0):
        super().__init__(timeout=120)
        self.roles = roles
        self.page = page
        self.per_page = 25

        start = page * self.per_page
        end = start + self.per_page
        self.add_item(LoveRoleSelect(roles[start:end], page))

        max_page = max(0, (len(roles) - 1) // self.per_page)
        if max_page > 0:
            prev_button = discord.ui.Button(
                label="Назад",
                style=discord.ButtonStyle.secondary,
                disabled=page <= 0,
            )
            next_button = discord.ui.Button(
                label="Дальше",
                style=discord.ButtonStyle.secondary,
                disabled=page >= max_page,
            )

            async def prev_callback(interaction: discord.Interaction):
                await interaction.response.edit_message(
                    content=f"Выбери роль из списка. Страница {page} / {max_page + 1}",
                    view=LoveRoleView(self.roles, page - 1),
                )

            async def next_callback(interaction: discord.Interaction):
                await interaction.response.edit_message(
                    content=f"Выбери роль из списка. Страница {page + 2} / {max_page + 1}",
                    view=LoveRoleView(self.roles, page + 1),
                )

            prev_button.callback = prev_callback
            next_button.callback = next_callback
            self.add_item(prev_button)
            self.add_item(next_button)


# =========================
# COMMANDS
# =========================
@bot.tree.command(
    name="dota_profile",
    description="Показать Dota-профиль по account_id, steamid64 или Steam-ссылке",
)
@discord.app_commands.describe(profile="account_id, steamid64 или ссылка Steam профиля")
async def dota_profile_command(interaction: discord.Interaction, profile: str):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        kind, value = parse_dota_profile_input(profile)

        steam64 = None
        if kind == "vanity":
            if not STEAM_API_KEY or STEAM_API_KEY == "PASTE_STEAM_API_KEY_HERE":
                await interaction.followup.send(
                    "Для vanity-ссылок нужно указать STEAM_API_KEY в конфиге.",
                    ephemeral=True,
                )
                return
            steam64 = resolve_vanity_to_steam64(value)
            account_id = steam64_to_account_id(steam64)

        elif kind == "steam64":
            steam64 = int(value)
            account_id = steam64_to_account_id(steam64)

        elif kind == "account_id":
            account_id = int(value)

        else:
            await interaction.followup.send(
                "Пришли account_id, steamid64 или Steam-ссылку.",
                ephemeral=True,
            )
            return

        player = opendota_get(f"/players/{account_id}")
        wl = opendota_get(f"/players/{account_id}/wl")
        totals = opendota_get(f"/players/{account_id}/totals")
        recent_matches = opendota_get(f"/players/{account_id}/recentMatches")
        top_heroes = opendota_get(f"/players/{account_id}/heroes")

        steam_summary = None
        if steam64 and STEAM_API_KEY and STEAM_API_KEY != "PASTE_STEAM_API_KEY_HERE":
            try:
                steam_summary = get_steam_summary(steam64)
            except Exception:
                steam_summary = None

        profile_data = player.get("profile", {}) or {}
        personaname = (
            (steam_summary or {}).get("personaname")
            or profile_data.get("personaname")
            or f"Player {account_id}"
        )
        avatar = (steam_summary or {}).get("avatarfull") or profile_data.get(
            "avatarfull"
        )

        win = int(wl.get("win", 0) or 0)
        lose = int(wl.get("lose", 0) or 0)
        total_matches = win + lose
        winrate = safe_winrate(win, lose)

        rank_text = rank_tier_to_text(player.get("rank_tier"))
        leaderboard_rank = player.get("leaderboard_rank")
        if leaderboard_rank:
            rank_text += f" (#{leaderboard_rank})"

        kills = extract_total_field(totals, "kills")
        deaths = extract_total_field(totals, "deaths")
        assists = extract_total_field(totals, "assists")

        avg_k = kills / total_matches if total_matches else 0
        avg_d = deaths / total_matches if total_matches else 0
        avg_a = assists / total_matches if total_matches else 0

        embed = discord.Embed(
            title=f"Dota профиль — {personaname}",
            colour=discord.Colour.dark_gold(),
        )

        embed.add_field(name="Ранг", value=rank_text, inline=True)
        embed.add_field(name="Матчи", value=str(total_matches), inline=True)
        embed.add_field(name="Винрейт", value=f"{winrate:.1f}%", inline=True)
        embed.add_field(name="W / L", value=f"{win} / {lose}", inline=True)
        embed.add_field(
            name="Средний KDA",
            value=f"{avg_k:.1f} / {avg_d:.1f} / {avg_a:.1f}",
            inline=True,
        )
        embed.add_field(name="Account ID", value=str(account_id), inline=True)

        if steam64:
            embed.add_field(name="SteamID64", value=str(steam64), inline=False)

        if top_heroes:
            hero_lines = []
            for hero in top_heroes[:3]:
                hero_id = hero.get("hero_id", "?")
                hero_name = HERO_NAMES.get(hero_id, f"Hero {hero_id}")
                games = hero.get("games", 0)
                wins = hero.get("win", 0)
                wr = (wins / games * 100) if games else 0
                hero_lines.append(f"{hero_name} — {games} игр, {wr:.1f}% WR")
            embed.add_field(
                name="Топ 3 героя", value="\n".join(hero_lines), inline=False
            )

        if recent_matches:
            match_lines = []

            for match in recent_matches[:5]:
                hero_id = int(match.get("hero_id", 0) or 0)
                hero_name = HERO_NAMES.get(hero_id, f"Hero {hero_id}")

                kills = int(match.get("kills", 0) or 0)
                deaths = int(match.get("deaths", 0) or 0)
                assists = int(match.get("assists", 0) or 0)

                player_slot = match.get("player_slot", 0)
                radiant_win = match.get("radiant_win", False)

                is_radiant = player_slot < 128
                result = (
                    "W"
                    if (is_radiant and radiant_win)
                    or (not is_radiant and not radiant_win)
                    else "L"
                )

                match_lines.append(
                    f"{result} | {hero_name} | {kills}/{deaths}/{assists}"
                )

            embed.add_field(
                name="Последние 5 матчей", value="\n".join(match_lines), inline=False
            )

        if avatar:
            embed.set_thumbnail(url=avatar)

        await interaction.followup.send(embed=embed, ephemeral=True)

    except requests.HTTPError as e:
        await interaction.followup.send(
            f"Ошибка запроса к API: {e}",
            ephemeral=True,
        )

    except Exception as e:
        await interaction.followup.send(
            f"Не удалось получить профиль: {e}",
            ephemeral=True,
        )


@bot.tree.command(name="lav3", description="Выдать себе роль main character")
async def lav3_command(interaction: discord.Interaction):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.",
            ephemeral=True,
        )
        return

    guild = interaction.guild
    if guild is None or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "Команда доступна только на сервере.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        role = await get_or_create_main_character_role(guild)
    except discord.Forbidden:
        await interaction.followup.send(
            "Не хватает прав создать/настроить роль. Дай боту Manage Roles и подними роль бота максимально высоко.",
            ephemeral=True,
        )
        return
    except discord.HTTPException as e:
        await interaction.followup.send(f"Не удалось создать/настроить роль: {e}", ephemeral=True)
        return

    if role in interaction.user.roles:
        nick_ok, nick_error = await ensure_main_character_nick(interaction.user)
        hint = get_main_character_role_status_hint(guild, role, interaction.user)
        message = f"У тебя уже есть роль **{role.name}**."
        if nick_ok:
            message += "\n👑 Приписку к нику тоже проверил/добавил."
        elif nick_error:
            message += "\n" + nick_error
        if hint:
            message += "\n\n" + hint
        await interaction.followup.send(
            message,
            ephemeral=True,
        )
        return

    if not bot_can_manage_role(guild, role):
        await interaction.followup.send(
            "Роль создана/найдена, но я не могу её выдать. Подними роль бота выше **main character**.",
            ephemeral=True,
        )
        return

    try:
        await interaction.user.add_roles(role, reason="/lav3 main character role")
        nick_ok, nick_error = await ensure_main_character_nick(interaction.user)
        hint = get_main_character_role_status_hint(guild, role, interaction.user)
        message = f"Готово, выдал тебе роль **{role.name}**."
        if nick_ok:
            message += "\n👑 Добавил приписку к нику."
        elif nick_error:
            message += "\n" + nick_error
        if hint:
            message += "\n\n" + hint
        await interaction.followup.send(
            message,
            ephemeral=True,
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "Не хватает прав выдать роль. Подними роль бота выше **main character**.",
            ephemeral=True,
        )
    except discord.HTTPException as e:
        await interaction.followup.send(f"Не удалось выдать роль: {e}", ephemeral=True)


@bot.tree.command(name="love", description="Выбрать и выдать себе роль из списка")
async def love_command(interaction: discord.Interaction):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.",
            ephemeral=True,
        )
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "Команда доступна только на сервере.",
            ephemeral=True,
        )
        return

    roles = get_self_assignable_roles(guild)
    if not roles:
        await interaction.response.send_message(
            "Нет ролей, которые я могу выдать. Проверь, что у бота есть Manage Roles и его роль выше нужных ролей.",
            ephemeral=True,
        )
        return

    max_page = max(0, (len(roles) - 1) // 25)
    await interaction.response.send_message(
        f"Выбери роль из списка. Страница 1 / {max_page + 1}",
        view=LoveRoleView(roles),
        ephemeral=True,
    )


@bot.tree.command(name="panel", description="Опубликовать панель заявок")
async def panel_command(interaction: discord.Interaction):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.",
            ephemeral=True,
        )
        return

    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда доступна только на сервере.",
            ephemeral=True,
        )
        return

    channel = interaction.guild.get_channel(PANEL_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message(
            "PANEL_CHANNEL_ID указан неверно.",
            ephemeral=True,
        )
        return

    try:
        async for message in channel.history(limit=50):
            if message.author == bot.user:
                await message.delete()
    except discord.HTTPException:
        pass

    embed = discord.Embed(
        title="Заявка в приватный войс",
        description="Нажми кнопку ниже, чтобы отправить заявку.",
        colour=discord.Colour.green(),
    )
    await channel.send(embed=embed, view=ApplyView())
    await interaction.response.send_message("Панель опубликована.", ephemeral=True)


@bot.tree.command(
    name="force_room", description="Создать приватный войс для пользователя"
)
@discord.app_commands.describe(member="Кому создать комнату")
async def force_room(interaction: discord.Interaction, member: discord.Member):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "Ошибка проверки прав.",
            ephemeral=True,
        )
        return

    if not any(role.id in STAFF_ROLE_IDS for role in interaction.user.roles):
        await interaction.response.send_message(
            "У тебя нет прав на эту команду.",
            ephemeral=True,
        )
        return

    await handle_approved_application(
        interaction.guild, member, reviewer=f"force by {interaction.user}"
    )
    await interaction.response.send_message(
        f"Готово. Комната для {member.mention} создана.",
        ephemeral=True,
    )


@bot.tree.command(name="refresh_mmr", description="Обновить статусы MMR во всех войсах")
async def refresh_mmr(interaction: discord.Interaction):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.",
            ephemeral=True,
        )
        return

    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда доступна только на сервере.",
            ephemeral=True,
        )
        return

    await update_all_tracked_voice_statuses(interaction.guild, force=True)
    await interaction.response.send_message("MMR-статусы обновлены.", ephemeral=True)


@bot.tree.command(
    name="voicestats", description="Показать статистику активности в войсах"
)
@discord.app_commands.describe(member="Чью статистику показать")
async def voicestats_command(
    interaction: discord.Interaction, member: Optional[discord.Member] = None
):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.", ephemeral=True
        )
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "Команда доступна только на сервере.", ephemeral=True
        )
        return

    target = member or interaction.user
    stats = get_user_stats(target.id)

    embed = discord.Embed(
        title=f"Voice статистика — {target.display_name}",
        colour=discord.Colour.gold(),
    )
    embed.add_field(
        name="Провёл в войсах",
        value=format_duration(stats.get("total_voice_seconds", 0)),
        inline=False,
    )
    embed.add_field(
        name="Сколько раз заходил", value=str(stats.get("join_count", 0)), inline=True
    )
    embed.add_field(
        name="Топ с кем играл",
        value="\n".join(get_top_played_with_lines(guild, target.id)),
        inline=False,
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="voicetop", description="Топ активности в войсах")
async def voicetop_command(interaction: discord.Interaction):
    if interaction.guild_id != GUILD_ID:
        await interaction.response.send_message(
            "Эта команда доступна только на нужном сервере.", ephemeral=True
        )
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "Команда доступна только на сервере.", ephemeral=True
        )
        return

    ranked = sorted(
        voice_stats.items(),
        key=lambda item: item[1].get("total_voice_seconds", 0),
        reverse=True,
    )

    lines = []
    for index, (user_id_str, stats) in enumerate(ranked[:10], start=1):
        member = guild.get_member(int(user_id_str))
        name = member.mention if member else f"<@{user_id_str}>"
        lines.append(
            f"{index}. {name} — {format_duration(stats.get('total_voice_seconds', 0))} "
            f"| заходов: {stats.get('join_count', 0)}"
        )

    if not lines:
        lines = ["Пока нет данных"]

    embed = discord.Embed(
        title="Топ активности в войсах",
        description="\n".join(lines),
        colour=discord.Colour.blurple(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


# =========================
# EVENTS
# =========================
@bot.event
async def on_ready():
    load_hero_names()
    logger.info("Logged in as %s (%s)", bot.user, bot.user.id)

    load_voice_stats()
    load_dota_links()
    bot.add_view(ApplyView())

    try:
        synced = await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        logger.info("Synced %s guild commands", len(synced))
    except Exception as e:
        logger.exception("Failed to sync commands: %s", e)

    guild = get_guild()
    if guild:
        for channel in guild.voice_channels:
            for member in channel.members:
                if not member.bot:
                    active_voice_sessions[member.id] = {
                        "channel_id": channel.id,
                        "joined_at": utc_now().isoformat(),
                    }

        # На старте НЕ редактируем статусы войсов, а только запоминаем текущие.
        # Иначе после каждого рестарта можно получить пачку записей в аудит-логе.
        for channel in guild.channels:
            if is_tracked_voice_channel(channel):
                remember_current_voice_status(channel)

        if UPDATE_VOICE_STATUSES_ON_READY:
            await update_all_tracked_voice_statuses(guild, force=False)

        await ensure_persistent_roles(guild)


    if not hasattr(bot, "persistent_role_watchdog_started"):
        bot.persistent_role_watchdog_started = True
        bot.loop.create_task(persistent_role_watchdog())


@bot.event
async def setup_hook():
    guild_obj = discord.Object(id=GUILD_ID)
    bot.tree.copy_global_to(guild=guild_obj)


@bot.event
async def on_voice_state_update(
    member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
):
    # 1) Voice activity tracking
    if before.channel and (before.channel != after.channel):
        finish_voice_session(member, before.channel)

    if after.channel and (before.channel != after.channel):
        start_voice_session(member, after.channel)

    # 2) Если пользователь вошёл в свою временную комнату — перекидываем его в админку
    room_data = pending_rooms.get(member.id)
    if (
        room_data
        and after.channel
        and after.channel.id == room_data["voice_channel_id"]
    ):
        admin_room = member.guild.get_channel(ADMIN_ROOM_ID)
        if isinstance(admin_room, discord.VoiceChannel):
            try:
                await member.move_to(admin_room, reason="Temporary room -> admin room")
            except discord.HTTPException:
                logger.exception(
                    "Не удалось перенести пользователя %s в админку", member.id
                )

    # 3) Даём Discord чуть обновить состав участников в каналах
    await asyncio.sleep(0.5)

    # 4) Обновление MMR-статуса у старого и нового канала
    affected_channels = set()

    if before.channel and is_tracked_voice_channel(before.channel):
        affected_channels.add(before.channel)

    if after.channel and is_tracked_voice_channel(after.channel):
        affected_channels.add(after.channel)

    for channel in affected_channels:
        await update_voice_status(channel)

    # 5) Автоудаление пустых приватных комнат
    if (
        AUTO_DELETE_EMPTY_PRIVATE_VOICES
        and before.channel
        and before.channel.id in private_voice_owners
    ):
        if len(before.channel.members) == 0:
            owner_id = private_voice_owners.pop(before.channel.id, None)
            room_data = pending_rooms.pop(owner_id, None) if owner_id else None

            if room_data and room_data.get("private_role_id"):
                role = member.guild.get_role(room_data["private_role_id"])
                if role:
                    owner_member = (
                        member.guild.get_member(owner_id) if owner_id else None
                    )
                    if owner_member:
                        try:
                            await owner_member.remove_roles(
                                role, reason="Empty private voice cleanup"
                            )
                        except discord.HTTPException:
                            logger.exception(
                                "Не удалось снять временную роль %s", role.id
                            )

                    try:
                        await role.delete(reason="Empty private voice cleanup")
                    except discord.HTTPException:
                        logger.exception(
                            "Не удалось удалить временную роль %s", role.id
                        )

            try:
                await before.channel.delete(reason="Empty private voice cleanup")
            except discord.HTTPException:
                logger.exception(
                    "Не удалось удалить пустой приватный войс %s", before.channel.id
                )


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    guild = after.guild
    persistent_role = find_persistent_role(guild)

    if after.id in PERSISTENT_ROLE_USER_IDS and persistent_role is not None:
        before_has_persistent = persistent_role in before.roles
        after_has_persistent = persistent_role in after.roles

        if before_has_persistent and not after_has_persistent:
            try:
                await after.add_roles(
                    persistent_role,
                    reason="Restore persistent tech moderator role after removal",
                )
                logger.info(
                    "Постоянная роль %s была возвращена пользователю %s после снятия",
                    persistent_role.id,
                    after.id,
                )
            except discord.Forbidden:
                logger.error(
                    "Нет прав вернуть постоянную роль %s пользователю %s",
                    persistent_role.id,
                    after.id,
                )
            except discord.HTTPException as e:
                logger.error(
                    "Ошибка при возврате постоянной роли %s пользователю %s: %s",
                    persistent_role.id,
                    after.id,
                    e,
                )

    before_role_ids = {r.id for r in before.roles}
    after_role_ids = {r.id for r in after.roles}
    mmr_ids = set(MMR_ROLES.keys())

    # Если изменились именно MMR-роли
    if (before_role_ids & mmr_ids) != (after_role_ids & mmr_ids):

        affected_channels = set()

        if (
            before.voice
            and before.voice.channel
            and is_tracked_voice_channel(before.voice.channel)
        ):
            affected_channels.add(before.voice.channel)

        if (
            after.voice
            and after.voice.channel
            and is_tracked_voice_channel(after.voice.channel)
        ):
            affected_channels.add(after.voice.channel)

        for channel in affected_channels:
            await update_voice_status(channel)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    bot.run(TOKEN)
