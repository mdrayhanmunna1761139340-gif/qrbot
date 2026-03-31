# -*- coding: utf-8 -*-
import os
import re
import json
import asyncio
import random
import time

from telethon import TelegramClient, events, Button
import config
from auto_sender import auto_sender_loop
from session_login import (
    MAX_LOGIN_RETRIES,
    build_login_state,
    start_login_request,
    finish_code_login,
    finish_password_login,
    close_login_client,
    cleanup_temp_session,
    move_temp_session_to_final,
    pretty_login_error,
)
from admin import (
    is_admin,
    load_users_db,
    save_users_db,
    user_allowed,
    approved_users,
    banned_users,
    pending_users,
    build_user_panel_menu,
    build_pending_user_actions,
    build_approved_user_actions,
    build_banned_user_actions,
)

# =========================================================
# MAIN BOT CLIENT
# =========================================================
bot = TelegramClient("bot_control", config.API_ID, config.API_HASH)

# user_id -> {label: TelegramClient}
user_clients = {}

# user_id -> {label: bool}
session_running = {}

# user_id -> {label: bool}
auto_send_running = {}

# user_id -> {label: int}
session_delay = {}

# user_id -> {label: float}
next_send_time = {}

# user_id -> {label: set((msg_id, btn_text))}
clicked = {}

# upload flow
pending_states = {}

# login / relogin flow
login_states = {}

# delay set flow
delay_states = {}

# new session delayed start
NEW_SESSION_START_DELAY = 60  # 1 minute
pending_session_start = {}    # user_id -> {label: True}


# =========================================================
# HELPERS
# =========================================================
def normalize(s: str) -> str:
    return " ".join((s or "").split()).strip().lower()


def safe_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name or "user"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def make_key(msg_id: int, text: str):
    return (msg_id, normalize(text))


def ensure_user_maps(user_id: int):
    if user_id not in user_clients:
        user_clients[user_id] = {}
    if user_id not in session_running:
        session_running[user_id] = {}
    if user_id not in auto_send_running:
        auto_send_running[user_id] = {}
    if user_id not in session_delay:
        session_delay[user_id] = {}
    if user_id not in next_send_time:
        next_send_time[user_id] = {}
    if user_id not in clicked:
        clicked[user_id] = {}
    if user_id not in pending_session_start:
        pending_session_start[user_id] = {}


def get_user_root_by_sender(sender) -> str:
    name = sender.username or sender.first_name or "user"
    folder = f"{sender.id}_{safe_name(name)}"
    path = os.path.join(config.SESSIONS_DIR, folder)
    ensure_dir(path)
    return path


def get_session_folder(sender, label: str) -> str:
    root = get_user_root_by_sender(sender)
    path = os.path.join(root, safe_name(label))
    ensure_dir(path)
    return path


def get_session_file_path(sender, label: str) -> str:
    folder = get_session_folder(sender, label)
    return os.path.join(folder, "user.session")


def get_meta_file_path(sender, label: str) -> str:
    folder = get_session_folder(sender, label)
    return os.path.join(folder, "meta.json")


def save_meta(sender, label: str):
    data = {
        "user_id": sender.id,
        "name": sender.username or sender.first_name or "user",
        "owner_username": sender.username,
        "owner_first_name": sender.first_name,
        "owner_last_name": sender.last_name,
        "label": safe_name(label),
        "created_at": int(time.time()),
        "status": "processing"
    }
    with open(get_meta_file_path(sender, label), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def mark_meta_ready(sender, label: str, tg_user_id=None, username=None, full_name=None, phone=None):
    path = get_meta_file_path(sender, label)

    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}

    data.update({
        "target_user_id": str(tg_user_id) if tg_user_id else data.get("target_user_id"),
        "target_username": username,
        "target_full_name": full_name,
        "target_phone": phone,
        "status": "ready",
        "ready_at": int(time.time())
    })

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_user_root(folder_name: str):
    m = re.match(r"^(\d+)_(.+)$", folder_name)
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)


def session_status_text(user_id: int, label: str) -> str:
    if pending_session_start.get(user_id, {}).get(label):
        return (
            f"📁 Session: {label}\n"
            f"Status: PROCESSING...\n"
            f"⏳ Waiting {NEW_SESSION_START_DELAY}s before start"
        )

    st = "ON" if session_running[user_id].get(label, False) else "OFF"
    ast = "ON" if auto_send_running[user_id].get(label, False) else "OFF"
    dly = session_delay[user_id].get(label, config.SEND_DELAY)
    return (
        f"📁 Session: {label}\n"
        f"Session Status: {st}\n"
        f"Auto Send: {ast}\n"
        f"Delay: {dly}s"
    )


async def cancel_login_flow(user_id: int):
    state = login_states.get(user_id)
    if not state:
        return

    try:
        await close_login_client(state)
    except Exception:
        pass

    label = state.get("label")
    if label:
        cleanup_temp_session(user_id, label)

    login_states.pop(user_id, None)


async def notify_admins(text, buttons=None):
    for admin_id in config.ADMINS:
        try:
            await bot.send_message(admin_id, text, buttons=buttons)
        except Exception as e:
            print(f"[ADMIN NOTIFY ERROR] {admin_id} -> {e}")


async def guard_access(event) -> bool:
    user_id = event.sender_id

    if is_admin(user_id):
        return True

    if user_id in banned_users:
        await event.reply("🚫 আপনি ব্যান করা আছেন।")
        return False

    if not user_allowed(user_id):
        await event.reply("🔐 এই বট ব্যবহারের জন্য আগে admin approval লাগবে। /start দিন।")
        return False

    return True


async def get_user_display(uid: int) -> str:
    try:
        user = await bot.get_entity(uid)
        name = user.first_name or "NoName"
        username = f"@{user.username}" if user.username else "NoUsername"
        return f"{name} ({username}) | {uid}"
    except Exception:
        return str(uid)


async def build_users_list_menu(users_set, prefix: str):
    buttons = []
    for uid in sorted(users_set):
        display = await get_user_display(uid)
        text = display[:60]
        buttons.append([Button.inline(f"👤 {text}", f"{prefix}:{uid}".encode())])
    buttons.append([Button.inline("⬅️ Back", b"user_panel")])
    return buttons


# =========================================================
# MENUS
# =========================================================
def build_main_menu(user_id: int):
    buttons = [
        [Button.inline("📂 My Sessions", b"my_sessions")],
        [Button.inline("✅ All ON", b"all_on"), Button.inline("⛔ All OFF", b"all_off")],
        [Button.inline("🚀 All Auto ON", b"all_auto_on"), Button.inline("🛑 All Auto OFF", b"all_auto_off")],
        [Button.inline("📊 All Status", b"all_status")],
        [Button.inline("➕ Add Session File", b"add_session")],
        [Button.inline("🔐 Login New ID", b"ui_login_new"), Button.inline("🔄 Relogin Session", b"ui_relogin")],
    ]

    if is_admin(user_id):
        buttons.insert(1, [Button.inline("👥 User Panel", b"user_panel")])

    buttons.append([Button.inline("ℹ️ Help", b"help_menu")])
    return buttons


def build_sessions_menu(user_id: int):
    ensure_user_maps(user_id)
    buttons = []

    labels_set = set(user_clients[user_id].keys())
    labels_set.update(pending_session_start[user_id].keys())

    for label in sorted(labels_set):
        if pending_session_start[user_id].get(label):
            buttons.append([
                Button.inline(f"⏳ {label} [PROCESSING]", f"open:{label}".encode())
            ])
            continue

        st = "ON" if session_running[user_id].get(label, False) else "OFF"
        ast = "AUTO ON" if auto_send_running[user_id].get(label, False) else "AUTO OFF"
        dly = session_delay[user_id].get(label, config.SEND_DELAY)
        buttons.append([
            Button.inline(f"📁 {label} [{st} | {ast} | {dly}s]", f"open:{label}".encode())
        ])

    buttons.append([Button.inline("⬅️ Back", b"back_main")])
    return buttons


def build_session_actions(user_id: int, label: str):
    if pending_session_start.get(user_id, {}).get(label):
        return [
            [Button.inline("ℹ️ Status", f"status:{label}".encode())],
            [Button.inline("🗑 Delete", f"delete:{label}".encode())],
            [Button.inline("⬅️ Back", b"my_sessions")],
        ]

    auto_text = "🚀 AutoSend ON" if auto_send_running[user_id].get(label, False) else "🛑 AutoSend OFF"
    return [
        [
            Button.inline("🟢 ON", f"on:{label}".encode()),
            Button.inline("🔴 OFF", f"off:{label}".encode()),
        ],
        [Button.inline(auto_text, f"autosend:{label}".encode())],
        [Button.inline("⏱ Set Delay", f"setdelay:{label}".encode())],
        [Button.inline("ℹ️ Status", f"status:{label}".encode())],
        [Button.inline("🔄 Relogin", f"relogin_ui:{label}".encode())],
        [Button.inline("🗑 Delete", f"delete:{label}".encode())],
        [Button.inline("⬅️ Back", b"my_sessions")],
    ]


def build_cancel_menu():
    return [
        [Button.inline("❌ Cancel", b"cancel_flow")],
        [Button.inline("⬅️ Main Menu", b"back_main")],
    ]


def build_relogin_pick_menu(user_id: int):
    ensure_user_maps(user_id)
    buttons = []
    for label in sorted(user_clients[user_id].keys()):
        buttons.append([Button.inline(f"🔄 {label}", f"relogin_ui:{label}".encode())])
    buttons.append([Button.inline("⬅️ Back", b"back_main")])
    return buttons


async def show_main_menu(event, text="🤖 Main Menu"):
    await event.respond(text, buttons=build_main_menu(event.sender_id))


async def edit_or_answer(event, text, buttons=None):
    try:
        await event.edit(text, buttons=buttons)
    except Exception:
        await event.respond(text, buttons=buttons)


# =========================================================
# BUTTON CLICK LOGIC
# =========================================================
async def click_once(user_id: int, label: str, message, text: str) -> bool:
    key = make_key(message.id, text)

    if key in clicked[user_id][label]:
        return False

    if not getattr(message, "buttons", None):
        return False

    want = normalize(text)

    try:
        await asyncio.sleep(random.uniform(0.3, 1.2))

        for row in message.buttons:
            for btn in row:
                if normalize(getattr(btn, "text", "")) == want:
                    await message.click(text=btn.text)
                    clicked[user_id][label].add(key)

                    if config.DEBUG:
                        print(f"[{user_id}:{label}] clicked -> {btn.text}")

                    return True
    except Exception as e:
        print(f"[{user_id}:{label}] click error: {type(e).__name__}: {e}")

    return False


async def process_message(user_id: int, label: str, msg):
    text = normalize(getattr(msg, "raw_text", "") or "")

    if config.DEBUG:
        print(f"[MSG] {user_id}:{label} -> {text[:120]}")

    if normalize(config.STEP3_TRIGGER) in text:
        if await click_once(user_id, label, msg, config.STEP3_TEXT):
            return

    if normalize(config.STEP2_TRIGGER) in text:
        if await click_once(user_id, label, msg, config.STEP2_TEXT):
            return

    await click_once(user_id, label, msg, config.STEP1_TEXT)


# =========================================================
# USER SESSION START / LOAD
# =========================================================
def register_handlers(client: TelegramClient, owner_user_id: int, label: str):
    @client.on(events.NewMessage(chats=config.TARGET_CHAT))
    async def on_new(event):
        if not session_running.get(owner_user_id, {}).get(label, False):
            return
        await process_message(owner_user_id, label, event.message)

    @client.on(events.MessageEdited(chats=config.TARGET_CHAT))
    async def on_edit(event):
        if not session_running.get(owner_user_id, {}).get(label, False):
            return
        await process_message(owner_user_id, label, event.message)


async def start_session_for_user_id(user_id: int, label: str, session_file: str):
    ensure_user_maps(user_id)
    label = safe_name(label)

    if label in user_clients[user_id]:
        return False, f"Session '{label}' already loaded"

    if not os.path.exists(session_file):
        return False, "Session file not found"

    session_base = session_file[:-8] if session_file.endswith(".session") else session_file

    try:
        client = TelegramClient(session_base, config.API_ID, config.API_HASH)
        await client.start()

        user_clients[user_id][label] = client
        session_running[user_id][label] = True
        auto_send_running[user_id][label] = True
        session_delay[user_id][label] = config.SEND_DELAY
        next_send_time[user_id][label] = 0.0
        clicked[user_id][label] = set()

        register_handlers(client, user_id, label)

        asyncio.create_task(
            auto_sender_loop(
                user_id,
                label,
                client,
                session_running,
                auto_send_running,
                session_delay,
                next_send_time,
            )
        )

        me = await client.get_me()
        info = getattr(me, "username", None) or me.id

        # meta ready update
        try:
            sender_obj = await bot.get_entity(user_id)
            full_name = " ".join(
                x for x in [getattr(me, "first_name", None), getattr(me, "last_name", None)] if x
            ).strip() or "Unknown"

            mark_meta_ready(
                sender_obj,
                label,
                tg_user_id=getattr(me, "id", None),
                username=getattr(me, "username", None),
                full_name=full_name,
                phone=getattr(me, "phone", None),
            )
        except Exception as e:
            print(f"[META READY ERROR] {user_id}:{label} -> {e}")

        if config.DEBUG:
            print(f"[+] Loaded {user_id}:{label} -> {info}")

        return True, f"Loaded '{label}' as {info}"

    except Exception as e:
        return False, str(e)


async def delayed_start_session(user_id: int, label: str, session_file: str, notify_chat_id: int = None):
    """
    New label add/login হলে সাথে সাথে session ON করবে না.
    আগে 1 minute wait করবে, তারপর session start করবে.
    """
    ensure_user_maps(user_id)
    label = safe_name(label)

    pending_session_start[user_id][label] = True

    try:
        print(f"[DELAYED START] Waiting {NEW_SESSION_START_DELAY}s for {user_id}:{label}")
        await asyncio.sleep(NEW_SESSION_START_DELAY)

        if not pending_session_start.get(user_id, {}).get(label):
            print(f"[DELAYED START] Cancelled {user_id}:{label}")
            return

        ok, msg = await start_session_for_user_id(user_id, label, session_file)

        if notify_chat_id:
            try:
                if ok:
                    await bot.send_message(
                        notify_chat_id,
                        f"✅ Session ON after {NEW_SESSION_START_DELAY}s processing\n"
                        f"📁 Label: {label}\n"
                        f"📌 Result: {msg}"
                    )
                else:
                    await bot.send_message(
                        notify_chat_id,
                        f"❌ Session start failed after delay\n"
                        f"📁 Label: {label}\n"
                        f"📌 Error: {msg}"
                    )
            except Exception as e:
                print(f"[DELAYED NOTIFY ERROR] {e}")

    except Exception as e:
        print(f"[DELAYED START ERROR] {user_id}:{label} -> {type(e).__name__}: {e}")

    finally:
        try:
            pending_session_start[user_id].pop(label, None)
        except Exception:
            pass


async def autoload_all_sessions():
    ensure_dir(config.SESSIONS_DIR)

    for root_name in os.listdir(config.SESSIONS_DIR):
        user_root = os.path.join(config.SESSIONS_DIR, root_name)
        if not os.path.isdir(user_root):
            continue

        owner_user_id, _ = parse_user_root(root_name)
        if owner_user_id is None:
            continue

        for label in os.listdir(user_root):
            label_dir = os.path.join(user_root, label)
            if not os.path.isdir(label_dir):
                continue

            session_file = os.path.join(label_dir, "user.session")
            if not os.path.exists(session_file):
                continue

            ok, msg = await start_session_for_user_id(owner_user_id, label, session_file)
            print(f"[AUTOLOAD] {owner_user_id}:{label} -> {msg}")


# =========================================================
# SESSION DELETE
# =========================================================
async def delete_session_for_user(user_id: int, label: str):
    ensure_user_maps(user_id)
    label = safe_name(label)

    # processing অবস্থায় থাকলেও delete হবে
    if pending_session_start.get(user_id, {}).get(label):
        pending_session_start[user_id][label] = False
        pending_session_start[user_id].pop(label, None)

    if label in user_clients[user_id]:
        try:
            client = user_clients[user_id][label]
            await client.disconnect()
        except Exception:
            pass

        user_clients[user_id].pop(label, None)
        session_running[user_id].pop(label, None)
        auto_send_running[user_id].pop(label, None)
        session_delay[user_id].pop(label, None)
        next_send_time[user_id].pop(label, None)
        clicked[user_id].pop(label, None)

    found = False

    for root_name in os.listdir(config.SESSIONS_DIR):
        owner_id, _ = parse_user_root(root_name)
        if owner_id != user_id:
            continue

        label_dir = os.path.join(config.SESSIONS_DIR, root_name, label)
        if os.path.isdir(label_dir):
            found = True
            for file_name in os.listdir(label_dir):
                file_path = os.path.join(label_dir, file_name)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                except Exception:
                    pass
            try:
                os.rmdir(label_dir)
            except Exception:
                pass
            break

    if found or label in user_clients[user_id]:
        return True, f"Session deleted: {label}"

    return False, "Session not found"


# =========================================================
# COMMANDS
# =========================================================
@bot.on(events.NewMessage(pattern=r"^/start$"))
async def start_cmd(event):
    user_id = event.sender_id
    sender = await event.get_sender()

    if is_admin(user_id):
        await show_main_menu(event, "🤖 Welcome Admin\nনিচের button use করো")
        return

    if user_id in banned_users:
        await event.reply("🚫 আপনি ব্যান করা আছেন।")
        return

    if user_allowed(user_id):
        await show_main_menu(event, "🤖 Welcome\nনিচের button use করো")
        return

    if user_id in pending_users:
        await event.reply("📩 আপনার access request admin-এর কাছে পাঠানো হয়েছে। অনুমোদনের জন্য অপেক্ষা করুন।")
        return

    pending_users.add(user_id)
    save_users_db()

    name = getattr(sender, "first_name", "") or "Unknown"
    username = getattr(sender, "username", None)
    uname = f"@{username}" if username else "No username"

    text = (
        "📩 New User Access Request\n\n"
        f"Name: {name}\n"
        f"Username: {uname}\n"
        f"User ID: {user_id}\n\n"
        "Approve or Ban:"
    )

    buttons = [
        [
            Button.inline("✅ Approve", f"approve_user:{user_id}".encode()),
            Button.inline("🚫 Ban", f"ban_user:{user_id}".encode()),
        ]
    ]

    await notify_admins(text, buttons=buttons)
    await event.reply("📩 আপনার access request admin-এর কাছে পাঠানো হয়েছে। অনুমোদনের জন্য অপেক্ষা করুন।")


@bot.on(events.NewMessage(pattern=r"^/help$"))
async def help_cmd(event):
    if not await guard_access(event):
        return

    text = (
        "UI buttons use করলেই হবে.\n\n"
        "Optional commands:\n"
        "/start\n"
        "/help\n"
        "/cancel\n"
    )
    await event.reply(text)


@bot.on(events.NewMessage(pattern=r"^/cancel$"))
async def cancel_cmd(event):
    if not await guard_access(event):
        return

    user_id = event.sender_id
    await cancel_login_flow(user_id)
    pending_states.pop(user_id, None)
    delay_states.pop(user_id, None)
    await event.reply("❌ Current flow cancelled.")


# =========================================================
# UI CALLBACKS
# =========================================================
@bot.on(events.CallbackQuery)
async def callbacks(event):
    user_id = event.sender_id
    ensure_user_maps(user_id)

    data = event.data.decode(errors="ignore")

    if data.startswith("approve_user:"):
        if not is_admin(user_id):
            await event.answer("Not allowed", alert=True)
            return

        target_user = int(data.split(":", 1)[1])

        pending_users.discard(target_user)
        banned_users.discard(target_user)
        approved_users.add(target_user)
        save_users_db()

        await event.answer("Approved")
        await edit_or_answer(event, f"✅ User approved: {target_user}", build_user_panel_menu())

        try:
            await bot.send_message(target_user, "✅ আপনার access approve করা হয়েছে। এখন /start দিন।")
        except Exception as e:
            print(f"[APPROVE NOTIFY ERROR] {target_user} -> {e}")
        return

    if data.startswith("ban_user:"):
        if not is_admin(user_id):
            await event.answer("Not allowed", alert=True)
            return

        target_user = int(data.split(":", 1)[1])

        pending_users.discard(target_user)
        approved_users.discard(target_user)
        banned_users.add(target_user)
        save_users_db()

        await event.answer("Banned")
        await edit_or_answer(event, f"🚫 User banned: {target_user}", build_user_panel_menu())

        try:
            await bot.send_message(target_user, "🚫 আপনার access বাতিল করা হয়েছে।")
        except Exception as e:
            print(f"[BAN NOTIFY ERROR] {target_user} -> {e}")
        return

    if data.startswith("unban_user:"):
        if not is_admin(user_id):
            await event.answer("Not allowed", alert=True)
            return

        target_user = int(data.split(":", 1)[1])

        banned_users.discard(target_user)
        pending_users.add(target_user)
        save_users_db()

        await event.answer("Unbanned")
        await edit_or_answer(event, f"✅ User unbanned: {target_user}", build_user_panel_menu())

        try:
            await bot.send_message(target_user, "✅ আপনার ban remove করা হয়েছে। আবার /start দিন approval-এর জন্য।")
        except Exception as e:
            print(f"[UNBAN NOTIFY ERROR] {target_user} -> {e}")
        return

    if not is_admin(user_id):
        if user_id in banned_users:
            await event.answer("🚫 Banned", alert=True)
            return
        if not user_allowed(user_id):
            await event.answer("🔐 Approval required", alert=True)
            return

    if data == "back_main":
        await event.answer()
        await edit_or_answer(event, "🤖 Main Menu", build_main_menu(user_id))
        return

    if data == "cancel_flow":
        await event.answer()
        await cancel_login_flow(user_id)
        pending_states.pop(user_id, None)
        delay_states.pop(user_id, None)
        await edit_or_answer(event, "❌ Flow cancelled.", build_main_menu(user_id))
        return

    if data == "help_menu":
        await event.answer()
        text = (
            "UI Features:\n"
            "• Add Session File\n"
            "• Login New ID\n"
            "• Relogin Session\n"
            "• My Sessions\n"
            "• Session ON/OFF\n"
            "• AutoSend ON/OFF\n"
            "• Set Delay\n"
            "• Delete Session\n"
            "• All ON/OFF\n"
            "• All Auto ON/OFF\n"
            "• Admin User Panel"
        )
        await edit_or_answer(event, text, build_main_menu(user_id))
        return

    if data == "user_panel":
        await event.answer()
        if not is_admin(user_id):
            await event.answer("Not allowed", alert=True)
            return
        await edit_or_answer(event, "👥 User Control Panel", build_user_panel_menu())
        return

    if data == "users_pending":
        await event.answer()
        if not is_admin(user_id):
            return

        if not pending_users:
            await edit_or_answer(event, "⏳ Pending user নেই", build_user_panel_menu())
            return

        await edit_or_answer(event, "⏳ Pending Users", await build_users_list_menu(pending_users, "view_pending"))
        return

    if data == "users_approved":
        await event.answer()
        if not is_admin(user_id):
            return

        if not approved_users:
            await edit_or_answer(event, "✅ Approved user নেই", build_user_panel_menu())
            return

        await edit_or_answer(event, "✅ Approved Users", await build_users_list_menu(approved_users, "view_approved"))
        return

    if data == "users_banned":
        await event.answer()
        if not is_admin(user_id):
            return

        if not banned_users:
            await edit_or_answer(event, "🚫 Banned user নেই", build_user_panel_menu())
            return

        await edit_or_answer(event, "🚫 Banned Users", await build_users_list_menu(banned_users, "view_banned"))
        return

    if data.startswith("view_pending:"):
        await event.answer()
        if not is_admin(user_id):
            return

        target_user = int(data.split(":", 1)[1])
        display = await get_user_display(target_user)
        text = f"👤 Pending User\n{display}"
        await edit_or_answer(event, text, build_pending_user_actions(target_user))
        return

    if data.startswith("view_approved:"):
        await event.answer()
        if not is_admin(user_id):
            return

        target_user = int(data.split(":", 1)[1])
        display = await get_user_display(target_user)
        text = f"👤 Approved User\n{display}"
        await edit_or_answer(event, text, build_approved_user_actions(target_user))
        return

    if data.startswith("view_banned:"):
        await event.answer()
        if not is_admin(user_id):
            return

        target_user = int(data.split(":", 1)[1])
        display = await get_user_display(target_user)
        text = f"👤 Banned User\n{display}"
        await edit_or_answer(event, text, build_banned_user_actions(target_user))
        return

    if data == "my_sessions":
        await event.answer()
        if not user_clients[user_id] and not pending_session_start[user_id]:
            await edit_or_answer(
                event,
                "No session found.\n➕ Add Session File বা 🔐 Login New ID use করো",
                build_main_menu(user_id)
            )
            return

        await edit_or_answer(event, "📂 Your Sessions", build_sessions_menu(user_id))
        return

    if data == "all_on":
        await event.answer()
        if not user_clients[user_id]:
            await edit_or_answer(event, "No active session found.", build_main_menu(user_id))
            return

        for label in user_clients[user_id]:
            session_running[user_id][label] = True

        await edit_or_answer(event, "✅ All your sessions ON", build_main_menu(user_id))
        return

    if data == "all_off":
        await event.answer()
        if not user_clients[user_id]:
            await edit_or_answer(event, "No active session found.", build_main_menu(user_id))
            return

        for label in user_clients[user_id]:
            session_running[user_id][label] = False

        await edit_or_answer(event, "⛔ All your sessions OFF", build_main_menu(user_id))
        return

    if data == "all_auto_on":
        await event.answer()
        if not user_clients[user_id]:
            await edit_or_answer(event, "No active session found.", build_main_menu(user_id))
            return

        for label in user_clients[user_id]:
            auto_send_running[user_id][label] = True
            delay = session_delay[user_id].get(label, config.SEND_DELAY)
            next_send_time[user_id][label] = time.monotonic() + delay

        await edit_or_answer(event, "🚀 All session auto send ON", build_main_menu(user_id))
        return

    if data == "all_auto_off":
        await event.answer()
        if not user_clients[user_id]:
            await edit_or_answer(event, "No active session found.", build_main_menu(user_id))
            return

        for label in user_clients[user_id]:
            auto_send_running[user_id][label] = False

        await edit_or_answer(event, "🛑 All session auto send OFF", build_main_menu(user_id))
        return

    if data == "all_status":
        await event.answer()
        if not user_clients[user_id] and not pending_session_start[user_id]:
            await edit_or_answer(event, "No session found.", build_main_menu(user_id))
            return

        lines = ["📊 Your Sessions:"]

        labels_set = set(user_clients[user_id].keys())
        labels_set.update(pending_session_start[user_id].keys())

        for label in sorted(labels_set):
            if pending_session_start[user_id].get(label):
                lines.append(f"- {label}: PROCESSING...")
                continue

            st = "ON" if session_running[user_id].get(label, False) else "OFF"
            ast = "ON" if auto_send_running[user_id].get(label, False) else "OFF"
            dly = session_delay[user_id].get(label, config.SEND_DELAY)
            lines.append(f"- {label}: Session={st} | AutoSend={ast} | Delay={dly}s")

        await edit_or_answer(event, "\n".join(lines), build_main_menu(user_id))
        return

    if data == "add_session":
        await event.answer()
        pending_states[user_id] = {"step": "label"}
        await edit_or_answer(
            event,
            "➕ নতুন session label পাঠাও\nউদাহরণ: acc1",
            build_cancel_menu()
        )
        return

    if data == "ui_login_new":
        await event.answer()
        await cancel_login_flow(user_id)
        login_states[user_id] = build_login_state(label="new_session", replace=False)
        await edit_or_answer(
            event,
            "🔐 New login start\nআগে session label পাঠাও\nউদাহরণ: acc1",
            build_cancel_menu()
        )
        return

    if data == "ui_relogin":
        await event.answer()
        if not user_clients[user_id]:
            await edit_or_answer(event, "No session found for relogin.", build_main_menu(user_id))
            return

        await edit_or_answer(
            event,
            "🔄 কোন session relogin করবে?",
            build_relogin_pick_menu(user_id)
        )
        return

    if data.startswith("relogin_ui:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])
        await cancel_login_flow(user_id)

        login_states[user_id] = build_login_state(label=label, replace=True)
        login_states[user_id]["step"] = "phone"

        await edit_or_answer(
            event,
            f"🔄 Relogin start for: {label}\nএখন phone number পাঠাও\nউদাহরণ: +8801XXXXXXXXX",
            build_cancel_menu()
        )
        return

    if data.startswith("open:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])

        labels_set = set(user_clients[user_id].keys())
        labels_set.update(pending_session_start[user_id].keys())

        if label not in labels_set:
            await edit_or_answer(event, "Session not found.", build_sessions_menu(user_id))
            return

        await edit_or_answer(event, session_status_text(user_id, label), build_session_actions(user_id, label))
        return

    if data.startswith("on:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])

        if label not in user_clients[user_id]:
            await edit_or_answer(event, "Session not found or still processing.", build_sessions_menu(user_id))
            return

        session_running[user_id][label] = True
        await edit_or_answer(event, session_status_text(user_id, label), build_session_actions(user_id, label))
        return

    if data.startswith("off:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])

        if label not in user_clients[user_id]:
            await edit_or_answer(event, "Session not found or still processing.", build_sessions_menu(user_id))
            return

        session_running[user_id][label] = False
        await edit_or_answer(event, session_status_text(user_id, label), build_session_actions(user_id, label))
        return

    if data.startswith("autosend:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])

        if label not in user_clients[user_id]:
            await edit_or_answer(event, "Session not found or still processing.", build_sessions_menu(user_id))
            return

        auto_send_running[user_id][label] = not auto_send_running[user_id].get(label, False)

        if auto_send_running[user_id][label]:
            delay = session_delay[user_id].get(label, config.SEND_DELAY)
            next_send_time[user_id][label] = time.monotonic() + delay

        await edit_or_answer(event, session_status_text(user_id, label), build_session_actions(user_id, label))
        return

    if data.startswith("setdelay:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])

        if label not in user_clients[user_id]:
            await edit_or_answer(event, "Session not found or still processing.", build_sessions_menu(user_id))
            return

        delay_states[user_id] = {"label": label}
        await edit_or_answer(
            event,
            f"⏱ {label} এর delay কত second?\nশুধু number পাঠাও\nউদাহরণ: 70",
            build_cancel_menu()
        )
        return

    if data.startswith("status:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])

        labels_set = set(user_clients[user_id].keys())
        labels_set.update(pending_session_start[user_id].keys())

        if label not in labels_set:
            await edit_or_answer(event, "Session not found.", build_sessions_menu(user_id))
            return

        await edit_or_answer(event, session_status_text(user_id, label), build_session_actions(user_id, label))
        return

    if data.startswith("delete:"):
        await event.answer()
        label = safe_name(data.split(":", 1)[1])

        ok, msg = await delete_session_for_user(user_id, label)
        if ok:
            await edit_or_answer(event, msg, build_sessions_menu(user_id))
        else:
            await edit_or_answer(event, msg, build_sessions_menu(user_id))
        return


# =========================================================
# TEXT INPUT FLOW
# =========================================================
@bot.on(events.NewMessage)
async def handle_text_and_file(event):
    if event.out:
        return

    if not await guard_access(event):
        return

    user_id = event.sender_id
    ensure_user_maps(user_id)

    if user_id in delay_states and event.raw_text and not event.raw_text.startswith("/"):
        label = delay_states[user_id]["label"]
        text = event.raw_text.strip()

        if not text.isdigit():
            await event.reply("শুধু number পাঠাও\nউদাহরণ: 70")
            return

        delay = int(text)
        if delay < 1:
            await event.reply("Delay minimum 1 second.")
            return

        session_delay[user_id][label] = delay
        next_send_time[user_id][label] = time.monotonic() + delay
        delay_states.pop(user_id, None)

        await event.reply(f"⏱ Delay set for {label}: {delay} sec")
        await event.respond("📂 Your Sessions", buttons=build_sessions_menu(user_id))
        return

    if user_id in login_states and event.raw_text and not event.raw_text.startswith("/"):
        state = login_states[user_id]
        sender = await event.get_sender()

        if state["step"] == "phone" and state["label"] == "new_session":
            label = safe_name(event.raw_text)

            if label in user_clients.get(user_id, {}) or pending_session_start[user_id].get(label):
                await event.reply("এই label already আছে বা processing-এ আছে. অন্য label দাও.")
                return

            state["label"] = label
            state["step"] = "phone_input"

            await event.reply(
                f"📁 Label set: {label}\nএখন phone number পাঠাও\nউদাহরণ: +8801XXXXXXXXX",
                buttons=build_cancel_menu()
            )
            return

        if state["step"] in ("phone", "phone_input"):
            phone = event.raw_text.strip()
            label = state["label"]

            try:
                client, phone_code_hash = await start_login_request(user_id, label, phone)

                state["client"] = client
                state["phone"] = phone
                state["phone_code_hash"] = phone_code_hash
                state["step"] = "code"
                state["retries"] = 0

                await event.reply(
                    f"📩 OTP পাঠানো হয়েছে: {phone}\nএখন OTP code পাঠাও",
                    buttons=build_cancel_menu()
                )
            except Exception as e:
                await event.reply(f"❌ Failed to send OTP\n{pretty_login_error(e)}")
                await cancel_login_flow(user_id)

            return

        if state["step"] == "code":
            code = event.raw_text.strip()
            result = await finish_code_login(state, code)

            if result["ok"]:
                try:
                    await close_login_client(state)

                    if state.get("replace") and state["label"] in user_clients.get(user_id, {}):
                        try:
                            old_client = user_clients[user_id][state["label"]]
                            await old_client.disconnect()
                        except Exception:
                            pass

                        user_clients[user_id].pop(state["label"], None)
                        session_running[user_id].pop(state["label"], None)
                        auto_send_running[user_id].pop(state["label"], None)
                        session_delay[user_id].pop(state["label"], None)
                        next_send_time[user_id].pop(state["label"], None)
                        clicked[user_id].pop(state["label"], None)

                    final_session_file = move_temp_session_to_final(sender, user_id, state["label"])
                    save_meta(sender, state["label"])
                    login_states.pop(user_id, None)

                    await event.reply(
                        f"✅ Login success\n"
                        f"📁 Label: {state['label']}\n"
                        f"⏳ {NEW_SESSION_START_DELAY} sec processing হবে\n"
                        f"তারপর session ON হবে।"
                    )

                    asyncio.create_task(
                        delayed_start_session(
                            user_id=user_id,
                            label=state["label"],
                            session_file=final_session_file,
                            notify_chat_id=event.chat_id
                        )
                    )

                except Exception as e:
                    await event.reply(f"❌ Finalize failed\n{type(e).__name__}: {e}")
                    await cancel_login_flow(user_id)

                return

            if result["need_password"]:
                state["step"] = "password"
                state["retries"] = 0
                await event.reply("🔐 2FA password দাও", buttons=build_cancel_menu())
                return

            state["retries"] += 1
            if state["retries"] >= MAX_LOGIN_RETRIES:
                await event.reply("❌ OTP retry limit crossed. আবার শুরু করো.")
                await cancel_login_flow(user_id)
            else:
                left = MAX_LOGIN_RETRIES - state["retries"]
                await event.reply(
                    f"❌ {pretty_login_error(result['error'])}\nআবার code দাও\nRetries left: {left}",
                    buttons=build_cancel_menu()
                )
            return

        if state["step"] == "password":
            password = event.raw_text.strip()
            result = await finish_password_login(state, password)

            if result["ok"]:
                try:
                    await close_login_client(state)

                    if state.get("replace") and state["label"] in user_clients.get(user_id, {}):
                        try:
                            old_client = user_clients[user_id][state["label"]]
                            await old_client.disconnect()
                        except Exception:
                            pass

                        user_clients[user_id].pop(state["label"], None)
                        session_running[user_id].pop(state["label"], None)
                        auto_send_running[user_id].pop(state["label"], None)
                        session_delay[user_id].pop(state["label"], None)
                        next_send_time[user_id].pop(state["label"], None)
                        clicked[user_id].pop(state["label"], None)

                    final_session_file = move_temp_session_to_final(sender, user_id, state["label"])
                    save_meta(sender, state["label"])
                    login_states.pop(user_id, None)

                    await event.reply(
                        f"✅ 2FA login success\n"
                        f"📁 Label: {state['label']}\n"
                        f"⏳ {NEW_SESSION_START_DELAY} sec processing হবে\n"
                        f"তারপর session ON হবে।"
                    )

                    asyncio.create_task(
                        delayed_start_session(
                            user_id=user_id,
                            label=state["label"],
                            session_file=final_session_file,
                            notify_chat_id=event.chat_id
                        )
                    )

                except Exception as e:
                    await event.reply(f"❌ Finalize failed\n{type(e).__name__}: {e}")
                    await cancel_login_flow(user_id)

                return

            state["retries"] += 1
            if state["retries"] >= MAX_LOGIN_RETRIES:
                await event.reply("❌ 2FA retry limit crossed. আবার শুরু করো.")
                await cancel_login_flow(user_id)
            else:
                left = MAX_LOGIN_RETRIES - state["retries"]
                await event.reply(
                    f"❌ {pretty_login_error(result['error'])}\nআবার 2FA password দাও\nRetries left: {left}",
                    buttons=build_cancel_menu()
                )
            return

    if user_id in pending_states and pending_states[user_id]["step"] == "label":
        if event.raw_text and not event.raw_text.startswith("/"):
            label = safe_name(event.raw_text)

            if label in user_clients[user_id] or pending_session_start[user_id].get(label):
                await event.reply("এই label already আছে বা processing-এ আছে. অন্য label দাও.")
                return

            pending_states[user_id] = {
                "step": "file",
                "label": label,
            }
            await event.reply(
                f"📁 Label set: {label}\nএখন ওই account-এর .session file পাঠাও",
                buttons=build_cancel_menu()
            )
            return

    if user_id in pending_states and pending_states[user_id]["step"] == "file":
        if event.file:
            file_name = getattr(event.file, "name", "") or ""
            if not file_name.endswith(".session"):
                await event.reply("শুধু .session file পাঠাও")
                return

            sender = await event.get_sender()
            label = pending_states[user_id]["label"]
            save_path = get_session_file_path(sender, label)

            await event.download_media(file=save_path)
            save_meta(sender, label)
            pending_states.pop(user_id, None)

            await event.reply(
                f"⏳ Session file saved\n"
                f"📁 Label: {label}\n"
                f"🕒 {NEW_SESSION_START_DELAY} sec processing হবে\n"
                f"তারপর session ON হবে।"
            )

            asyncio.create_task(
                delayed_start_session(
                    user_id=user_id,
                    label=label,
                    session_file=save_path,
                    notify_chat_id=event.chat_id
                )
            )

            return


# =========================================================
# MAIN
# =========================================================
async def main():
    ensure_dir(config.SESSIONS_DIR)
    load_users_db()

    await autoload_all_sessions()

    await bot.start(bot_token=config.BOT_TOKEN)
    me = await bot.get_me()

    print(f"[+] Bot started as @{getattr(me, 'username', None) or me.id}")
    print("[+] Use /start")

    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())