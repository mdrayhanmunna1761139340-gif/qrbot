import os
import json
from telethon import Button
import config

USERS_DB = "users.json"

approved_users = set()
banned_users = set()
pending_users = set()


def is_admin(user_id: int) -> bool:
    return user_id in config.ADMINS


def load_users_db():
    global approved_users, banned_users, pending_users

    if not os.path.exists(USERS_DB):
        save_users_db()
        return

    try:
        with open(USERS_DB, "r", encoding="utf-8") as f:
            data = json.load(f)

        approved_users.clear()
        approved_users.update(data.get("approved", []))

        banned_users.clear()
        banned_users.update(data.get("banned", []))

        pending_users.clear()
        pending_users.update(data.get("pending", []))

    except Exception:
        approved_users.clear()
        banned_users.clear()
        pending_users.clear()


def save_users_db():
    data = {
        "approved": list(approved_users),
        "banned": list(banned_users),
        "pending": list(pending_users),
    }
    with open(USERS_DB, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def user_allowed(user_id: int) -> bool:
    return user_id in approved_users and user_id not in banned_users


def build_user_panel_menu():
    return [
        [Button.inline("⏳ Pending Users", b"users_pending")],
        [Button.inline("✅ Approved Users", b"users_approved")],
        [Button.inline("🚫 Banned Users", b"users_banned")],
        [Button.inline("⬅️ Back", b"back_main")],
    ]


def build_pending_user_actions(target_user: int):
    return [
        [
            Button.inline("✅ Approve", f"approve_user:{target_user}".encode()),
            Button.inline("🚫 Ban", f"ban_user:{target_user}".encode()),
        ],
        [Button.inline("⬅️ Back", b"users_pending")],
    ]


def build_approved_user_actions(target_user: int):
    return [
        [Button.inline("🚫 Ban", f"ban_user:{target_user}".encode())],
        [Button.inline("⬅️ Back", b"users_approved")],
    ]


def build_banned_user_actions(target_user: int):
    return [
        [Button.inline("✅ Unban", f"unban_user:{target_user}".encode())],
        [Button.inline("⬅️ Back", b"users_banned")],
    ]