# session_login.py
import os
import re
import shutil

from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    FloodWaitError,
)

import config

MAX_LOGIN_RETRIES = 3


def safe_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name or "user"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def get_user_root(sender) -> str:
    name = sender.username or sender.first_name or "user"
    folder = f"{sender.id}_{safe_name(name)}"
    path = os.path.join(config.SESSIONS_DIR, folder)
    ensure_dir(path)
    return path


def get_label_dir(sender, label: str) -> str:
    root = get_user_root(sender)
    path = os.path.join(root, safe_name(label))
    ensure_dir(path)
    return path


def get_final_session_base(sender, label: str) -> str:
    return os.path.join(get_label_dir(sender, label), "user")


def get_temp_session_dir() -> str:
    path = os.path.join(config.SESSIONS_DIR, "_temp_login")
    ensure_dir(path)
    return path


def get_temp_session_base(user_id: int, label: str) -> str:
    label = safe_name(label)
    return os.path.join(get_temp_session_dir(), f"{user_id}_{label}_temp")


def cleanup_temp_session(user_id: int, label: str):
    base = get_temp_session_base(user_id, label)
    for path in (base, base + ".session", base + ".session-journal"):
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass


def move_temp_session_to_final(sender, user_id: int, label: str):
    temp_base = get_temp_session_base(user_id, label)
    final_base = get_final_session_base(sender, label)

    temp_session = temp_base + ".session"
    temp_journal = temp_base + ".session-journal"

    final_dir = os.path.dirname(final_base)
    ensure_dir(final_dir)

    final_session = final_base + ".session"
    final_journal = final_base + ".session-journal"

    if not os.path.exists(temp_session):
        raise FileNotFoundError("Temp session file not found")

    if os.path.exists(final_session):
        os.remove(final_session)
    if os.path.exists(final_journal):
        os.remove(final_journal)

    shutil.move(temp_session, final_session)

    if os.path.exists(temp_journal):
        shutil.move(temp_journal, final_journal)

    return final_session


async def start_login_request(user_id: int, label: str, phone: str):
    cleanup_temp_session(user_id, label)

    temp_base = get_temp_session_base(user_id, label)
    client = TelegramClient(temp_base, config.API_ID, config.API_HASH)
    await client.connect()

    sent = await client.send_code_request(phone)
    return client, sent.phone_code_hash


async def finish_code_login(state: dict, code: str):
    client = state["client"]
    phone = state["phone"]
    phone_code_hash = state["phone_code_hash"]

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        me = await client.get_me()
        return {
            "ok": True,
            "need_password": False,
            "me": me,
            "error": None,
        }

    except SessionPasswordNeededError:
        return {
            "ok": False,
            "need_password": True,
            "me": None,
            "error": None,
        }

    except (PhoneCodeInvalidError, PhoneCodeExpiredError, FloodWaitError) as e:
        return {
            "ok": False,
            "need_password": False,
            "me": None,
            "error": e,
        }


async def finish_password_login(state: dict, password: str):
    client = state["client"]

    try:
        await client.sign_in(password=password)
        me = await client.get_me()
        return {
            "ok": True,
            "me": me,
            "error": None,
        }

    except (PasswordHashInvalidError, FloodWaitError) as e:
        return {
            "ok": False,
            "me": None,
            "error": e,
        }


async def close_login_client(state: dict):
    client = state.get("client")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass


def build_login_state(label: str, phone: str = "", replace: bool = False):
    return {
        "flow": "login",
        "step": "phone",
        "label": safe_name(label),
        "phone": phone,
        "replace": replace,
        "retries": 0,
        "client": None,
        "phone_code_hash": None,
    }


def pretty_login_error(e: Exception) -> str:
    if isinstance(e, FloodWaitError):
        return f"Too many attempts. Wait {e.seconds} seconds."
    if isinstance(e, PhoneCodeInvalidError):
        return "Invalid OTP code."
    if isinstance(e, PhoneCodeExpiredError):
        return "OTP expired. Start login again."
    if isinstance(e, PasswordHashInvalidError):
        return "Wrong 2FA password."
    return f"{type(e).__name__}: {e}"