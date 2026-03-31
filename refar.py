# -- coding: utf-8 --
import os
import re
import json
import time
import shutil
import sqlite3
import asyncio
from typing import Dict, List, Set, Tuple, Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError

# =========================================================
# CONFIG
# =========================================================
API_ID = 35324324
API_HASH = "9964384cb72bb739302d2889998e713c"

ADMIN_SESSION = "admin/admin"
TARGET_BOT = "WSTASKBOT"   # e.g. WSTASKBOT
REPORT_CHAT = -1003715932756
SESSIONS_DIR = "sessions"

DATA_DIR = "data"
KNOWN_LABELS_FILE = os.path.join(DATA_DIR, "known_labels.json")
LOCK_FILE = os.path.join(DATA_DIR, "refar.lock")
SCRAPED_JSON = os.path.join(DATA_DIR, "scraped_teamactive.json")

TEAM_COMMAND = "👥 Team"
TEAMACTIVE_COMMAND = "/teamactive"

NEXT_BUTTON_TEXTS = ["➡️ Next", "Next", "➡ Next", "⏭ Next", "▶️ Next"]

NEW_LABEL_SCAN_INTERVAL = 20
SCHEDULED_RECHECK_SECONDS = 43200   # 12h
NEW_LABEL_CHECK_DELAY = 3

# reply wait
BOT_REPLY_WAIT_TIMEOUT = 90
BOT_REPLY_POLL_INTERVAL = 2

DEBUG = True

# =========================================================
# GLOBALS
# =========================================================
admin_client = TelegramClient(ADMIN_SESSION, API_ID, API_HASH)

# =========================================================
# UTILS
# =========================================================
def dbg(*args):
    if DEBUG:
        print(*args)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def normalize(s: str) -> str:
    return " ".join((s or "").split()).strip().lower()

def load_json(path: str, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        dbg(f"[JSON LOAD ERROR] {path} -> {e}")
    return default

def save_json(path: str, data):
    try:
        ensure_dir(os.path.dirname(path) or ".")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        dbg(f"[JSON SAVE ERROR] {path} -> {e}")

def fmt_ts(ts: float) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return "Unknown"

def chunk_text(text: str, size: int = 3500) -> List[str]:
    return [text[i:i+size] for i in range(0, len(text), size)] or [""]

def acquire_lock() -> bool:
    ensure_dir(DATA_DIR)
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r", encoding="utf-8") as f:
                old = f.read().strip()
            dbg(f"[LOCK] Already running? lock={old}")
        except Exception:
            pass
        return False

    try:
        with open(LOCK_FILE, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        dbg(f"[LOCK ERROR] {e}")
        return False

def release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass

def parse_user_root(folder_name: str) -> Tuple[Optional[int], Optional[str]]:
    m = re.match(r"^(\d+)_(.+)$", folder_name)
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)

def get_meta_file_path(label_dir: str) -> str:
    return os.path.join(label_dir, "meta.json")

def get_session_file_path(label_dir: str) -> str:
    return os.path.join(label_dir, "user.session")

def is_sqlite_locked(session_file: str) -> bool:
    try:
        con = sqlite3.connect(session_file, timeout=1)
        con.execute("SELECT name FROM sqlite_master LIMIT 1;")
        con.close()
        return False
    except sqlite3.OperationalError as e:
        return "locked" in str(e).lower()
    except Exception:
        return False

def make_temp_session_copy(session_file: str) -> Optional[str]:
    try:
        if not os.path.exists(session_file):
            return None

        temp_dir = os.path.join(DATA_DIR, "temp_sessions")
        ensure_dir(temp_dir)

        base_name = f"tmp_{int(time.time()*1000)}_{os.path.basename(session_file)}"
        temp_session = os.path.join(temp_dir, base_name)

        shutil.copy2(session_file, temp_session)

        for suffix in ["-journal", "-wal", "-shm"]:
            src = session_file + suffix
            dst = temp_session + suffix
            if os.path.exists(src):
                try:
                    shutil.copy2(src, dst)
                except Exception:
                    pass

        return temp_session
    except Exception as e:
        dbg(f"[TEMP COPY ERROR] {session_file} -> {e}")
        return None

# =========================================================
# LABEL SESSION INFO
# =========================================================
async def read_logged_account_info(session_file: str) -> Dict:
    info = {
        "authorized": False,
        "locked": False,
        "user_id": None,
        "username": None,
        "full_name": None,
        "phone": None,
    }

    client = None
    temp_session = None

    try:
        if not os.path.exists(session_file):
            return info

        session_base = session_file[:-8] if session_file.endswith(".session") else session_file
        try_paths = [session_base]

        if is_sqlite_locked(session_file):
            info["locked"] = True
            temp_session = make_temp_session_copy(session_file)
            if temp_session:
                temp_base = temp_session[:-8] if temp_session.endswith(".session") else temp_session
                try_paths.insert(0, temp_base)

        for base in try_paths:
            try:
                client = TelegramClient(base, API_ID, API_HASH)
                await client.connect()

                if not await client.is_user_authorized():
                    await client.disconnect()
                    client = None
                    continue

                me = await client.get_me()

                info["authorized"] = True
                info["user_id"] = str(me.id) if me else None
                info["username"] = getattr(me, "username", None)
                info["phone"] = getattr(me, "phone", None)

                fn = getattr(me, "first_name", "") or ""
                ln = getattr(me, "last_name", "") or ""
                full_name = f"{fn} {ln}".strip()
                info["full_name"] = full_name if full_name else None

                await client.disconnect()
                client = None
                break

            except Exception as e:
                dbg(f"[SESSION READ TRY FAIL] {base} -> {type(e).__name__}: {e}")
                try:
                    if client:
                        await client.disconnect()
                except Exception:
                    pass
                client = None

    except Exception as e:
        dbg(f"[SESSION READ ERROR] {session_file} -> {type(e).__name__}: {e}")

    finally:
        try:
            if client:
                await client.disconnect()
        except Exception:
            pass

        if temp_session:
            try:
                os.remove(temp_session)
            except Exception:
                pass
            for suffix in ["-journal", "-wal", "-shm"]:
                try:
                    p = temp_session + suffix
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass

    return info

# =========================================================
# SCAN LABELS
# =========================================================
async def scan_all_labels() -> Dict[str, Dict]:
    results: Dict[str, Dict] = {}

    if not os.path.isdir(SESSIONS_DIR):
        dbg(f"[SCAN] No sessions dir: {SESSIONS_DIR}")
        return results

    for root_name in os.listdir(SESSIONS_DIR):
        root_path = os.path.join(SESSIONS_DIR, root_name)
        if not os.path.isdir(root_path):
            continue

        owner_id, owner_name = parse_user_root(root_name)
        if owner_id is None:
            continue

        for label in os.listdir(root_path):
            label_dir = os.path.join(root_path, label)
            if not os.path.isdir(label_dir):
                continue

            session_file = get_session_file_path(label_dir)
            if not os.path.exists(session_file):
                continue

            meta_file = get_meta_file_path(label_dir)
            meta = load_json(meta_file, {})
            owner_username = meta.get("owner_username") if isinstance(meta, dict) else None

            logged_info = await read_logged_account_info(session_file)

            results[label] = {
                "label": label,
                "owner_id": owner_id,
                "owner_name": owner_name,
                "owner_username": owner_username,
                "folder": label_dir,
                "session_file": session_file,
                "meta_file": meta_file,
                "last_modified": os.path.getmtime(session_file),

                "authorized": logged_info["authorized"],
                "locked": logged_info["locked"],
                "logged_user_id": logged_info["user_id"],
                "logged_username": logged_info["username"],
                "logged_name": logged_info["full_name"],
                "logged_phone": logged_info["phone"],
            }

    return results

# =========================================================
# BOT HELPERS
# =========================================================
async def safe_send_bot(text: str):
    try:
        await admin_client.send_message(TARGET_BOT, text)
    except FloodWaitError as e:
        dbg(f"[FLOODWAIT] send bot -> sleep {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        await admin_client.send_message(TARGET_BOT, text)

async def get_latest_bot_msg_id() -> int:
    msgs = await admin_client.get_messages(TARGET_BOT, limit=1)
    if msgs:
        return msgs[0].id
    return 0

def is_valid_teamactivity_text(text: str) -> bool:
    if not text:
        return False

    txt = text.lower()

    if "user id:" in txt and "successful sends:" in txt:
        return True

    if "team activity" in txt and "page" in txt:
        return True

    return False

async def wait_for_valid_teamactivity(after_msg_id: int, timeout: int = BOT_REPLY_WAIT_TIMEOUT):
    """
    /teamactive send করার পরে valid teamactivity message না আসা পর্যন্ত wait করবে
    """
    start = time.time()

    while time.time() - start < timeout:
        msgs = await admin_client.get_messages(TARGET_BOT, limit=10)

        for msg in msgs:
            if msg.id <= after_msg_id:
                continue

            text = msg.raw_text or ""

            if is_valid_teamactivity_text(text):
                dbg(f"[WAIT] Valid teamactivity message received: msg_id={msg.id}")
                return msg

        await asyncio.sleep(BOT_REPLY_POLL_INTERVAL)

    return None

async def click_button_by_any_text(message, wanted_texts: List[str]) -> bool:
    if not getattr(message, "buttons", None):
        return False

    wanted_norm = [normalize(x) for x in wanted_texts]

    for row in message.buttons:
        for btn in row:
            btn_text = normalize(getattr(btn, "text", ""))
            if btn_text in wanted_norm:
                try:
                    await message.click(text=btn.text)
                    dbg(f"[CLICK] Button clicked: {btn.text}")
                    return True
                except FloodWaitError as e:
                    dbg(f"[FLOODWAIT] Sleep {e.seconds}s")
                    await asyncio.sleep(e.seconds + 1)
                    return False
                except Exception as e:
                    dbg(f"[CLICK ERROR] {type(e).__name__}: {e}")
                    return False
    return False

def has_next_button(message) -> bool:
    if not getattr(message, "buttons", None):
        return False

    wanted_norm = [normalize(x) for x in NEXT_BUTTON_TEXTS]

    for row in message.buttons:
        for btn in row:
            btn_text = normalize(getattr(btn, "text", ""))
            if btn_text in wanted_norm:
                return True
    return False

# =========================================================
# WS TASK PARSER
# =========================================================
def parse_team_activity(text: str) -> Dict:
    users = []

    pattern = re.compile(
        r"User ID:\s*(\d+).*?"
        r"Successful sends:\s*(\d+).*?"
        r"Rebate\s+(received|not received)",
        re.I | re.S
    )

    for m in pattern.finditer(text or ""):
        uid = m.group(1)
        sends = int(m.group(2))
        rebate = m.group(3).lower().strip()

        users.append({
            "user_id": uid,
            "successful_sends": sends,
            "rebate": rebate
        })

    page_match = re.search(r"Page\s*(\d+)\s*/\s*(\d+)", text or "", re.I)
    page = int(page_match.group(1)) if page_match else None
    total_pages = int(page_match.group(2)) if page_match else None

    return {
        "page": page,
        "total_pages": total_pages,
        "users": users
    }

# =========================================================
# SCRAPER (MAIN FIX)
# =========================================================
async def scrape_ws_task_ids() -> Tuple[Set[str], List[Dict]]:
    found_users: List[Dict] = []
    found_ids: Set[str] = set()

    dbg("[SCRAPE] Opening Team...")
    await safe_send_bot(TEAM_COMMAND)
    await asyncio.sleep(2)

    dbg("[SCRAPE] Sending /teamactive...")
    before_id = await get_latest_bot_msg_id()
    await safe_send_bot(TEAMACTIVE_COMMAND)

    # IMPORTANT: valid teamactivity না আসা পর্যন্ত wait
    msg = await wait_for_valid_teamactivity(before_id, BOT_REPLY_WAIT_TIMEOUT)
    if not msg:
        dbg("[SCRAPE] No valid /teamactive reply received.")
        return found_ids, found_users

    visited_page_signatures = set()
    page_guard = 0

    while True:
        page_guard += 1
        if page_guard > 100:
            dbg("[SCRAPE] Page guard hit.")
            break

        text = msg.raw_text or ""
        parsed = parse_team_activity(text)

        # page signature
        sig = (
            parsed.get("page"),
            parsed.get("total_pages"),
            hash(text)
        )

        if sig in visited_page_signatures:
            dbg("[SCRAPE] Same page repeated, stop.")
            break

        visited_page_signatures.add(sig)

        # parse users
        for u in parsed["users"]:
            uid = str(u["user_id"])
            if uid not in found_ids:
                found_users.append(u)
                found_ids.add(uid)

        dbg(f"[SCRAPE] page={parsed.get('page')}/{parsed.get('total_pages')} users_total={len(found_users)}")

        # যদি next button না থাকে -> done
        if not has_next_button(msg):
            dbg("[SCRAPE] No Next button. Finished all pages.")
            break

        dbg("[SCRAPE] Next button found, clicking...")
        current_msg_id = msg.id
        clicked = await click_button_by_any_text(msg, NEXT_BUTTON_TEXTS)
        if not clicked:
            dbg("[SCRAPE] Next click failed.")
            break

        # next page valid msg wait
        next_msg = await wait_for_valid_teamactivity(current_msg_id, timeout=40)

        if not next_msg:
            dbg("[SCRAPE] No next page valid response.")
            break

        msg = next_msg

    save_json(SCRAPED_JSON, found_users)
    dbg(f"[SCRAPE DONE] total users scraped = {len(found_users)}")
    return found_ids, found_users

# =========================================================
# GROUP BY LOGGED USER
# =========================================================
def group_labels_by_logged_user(label_map: Dict[str, Dict]) -> Dict[str, List[Dict]]:
    grouped: Dict[str, List[Dict]] = {}

    for _, info in label_map.items():
        uid = str(info.get("logged_user_id") or "None")
        grouped.setdefault(uid, []).append(info)

    return grouped

# =========================================================
# COMPARE
# =========================================================
def compare_logged_users(label_map: Dict[str, Dict], found_users: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    found_by_id = {str(x["user_id"]): x for x in found_users}
    grouped = group_labels_by_logged_user(label_map)

    matched = []
    missing = []

    for uid, labels in grouped.items():
        first = labels[0]

        if not uid or uid == "None":
            missing.append({
                "logged_name": first.get("logged_name"),
                "logged_username": first.get("logged_username"),
                "logged_user_id": None,
                "logged_phone": first.get("logged_phone"),
                "authorized": first.get("authorized", False),
                "owner_id": first.get("owner_id"),
                "owner_name": first.get("owner_name"),
                "owner_username": first.get("owner_username"),
                "labels": labels,
                "status": "no_logged_user_id"
            })
            continue

        if uid in found_by_id:
            u = found_by_id[uid]
            matched.append({
                "logged_name": first.get("logged_name"),
                "logged_username": first.get("logged_username"),
                "logged_user_id": uid,
                "logged_phone": first.get("logged_phone"),
                "authorized": first.get("authorized", False),
                "owner_id": first.get("owner_id"),
                "owner_name": first.get("owner_name"),
                "owner_username": first.get("owner_username"),
                "labels": labels,
                "successful_sends": u.get("successful_sends", 0),
                "rebate": u.get("rebate", "unknown"),
                "status": "matched"
            })
        else:
            missing.append({
                "logged_name": first.get("logged_name"),
                "logged_username": first.get("logged_username"),
                "logged_user_id": uid,
                "logged_phone": first.get("logged_phone"),
                "authorized": first.get("authorized", False),
                "owner_id": first.get("owner_id"),
                "owner_name": first.get("owner_name"),
                "owner_username": first.get("owner_username"),
                "labels": labels,
                "status": "not_found"
            })

    return matched, missing

# =========================================================
# REPORT
# =========================================================
def build_user_block(item: Dict, matched: bool = True) -> str:
    lines = []

    if matched:
        lines.append("✅ MATCHED USER")
    else:
        lines.append("❌ NOT FOUND USER")

    lines.append("")
    lines.append(f"👤 Logged Account Name: {item.get('logged_name') or 'Unknown'}")
    lines.append(f"🌐 Logged Username: @{item.get('logged_username')}" if item.get("logged_username") else "🌐 Logged Username: None")
    lines.append(f"🆔 Logged User ID: {item.get('logged_user_id')}")
    lines.append(f"📱 Logged Phone: {item.get('logged_phone')}")
    lines.append(f"🔐 Session Authorized: {'Yes' if item.get('authorized') else 'No'}")
    lines.append("")
    lines.append("👑 Label Owner Info:")
    lines.append(f"🆔 Owner ID: {item.get('owner_id')}")
    lines.append(f"👤 Owner Name: {item.get('owner_name')}")
    lines.append(f"🌐 Owner Username: @{item.get('owner_username')}" if item.get("owner_username") else "🌐 Owner Username: None")
    lines.append("")
    lines.append(f"📦 Total Labels: {len(item.get('labels', []))}")
    lines.append("")
    lines.append("🗂 LABEL DETAILS:")
    lines.append("")

    for i, lb in enumerate(item.get("labels", []), start=1):
        lines.append(f"{i}) Label: {lb.get('label')}")
        lines.append(f"   📁 Folder: {lb.get('folder')}")
        lines.append(f"   💾 Session: {lb.get('session_file')}")
        lines.append(f"   📝 Meta: {lb.get('meta_file')}")
        lines.append(f"   🔐 Authorized: {'Yes' if lb.get('authorized') else 'No'}")
        lines.append(f"   🔒 Locked: {'Yes' if lb.get('locked') else 'No'}")
        lines.append(f"   🕒 Last Modified: {fmt_ts(lb.get('last_modified', 0))}")
        lines.append("")

    if matched:
        lines.append(f"📨 Sends: {item.get('successful_sends', 0)}")
        lines.append(f"🎁 Rebate: {item.get('rebate', 'unknown')}")
    else:
        lines.append(f"📌 Status: {item.get('status')}")

    lines.append("─" * 35)
    return "\n".join(lines)

async def send_report(title: str, matched: List[Dict], missing: List[Dict]):
    lines = [title, ""]

    lines.append(f"✅ MATCHED USERS: {len(matched)}")
    if matched:
        for item in matched:
            lines.append(build_user_block(item, matched=True))
    else:
        lines.append("— None")

    lines.append("")
    lines.append(f"❌ NOT FOUND USERS: {len(missing)}")
    if missing:
        for item in missing:
            lines.append(build_user_block(item, matched=False))
    else:
        lines.append("— None")

    text = "\n".join(lines).strip()

    for part in chunk_text(text, 3500):
        try:
            await admin_client.send_message(REPORT_CHAT, part)
        except FloodWaitError as e:
            dbg(f"[FLOODWAIT] report -> sleep {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
            await admin_client.send_message(REPORT_CHAT, part)
        except Exception as e:
            dbg(f"[REPORT SEND ERROR] {type(e).__name__}: {e}")

# =========================================================
# RUN CHECK
# =========================================================
async def run_check_for_labels(labels_subset: Dict[str, Dict], title: str):
    if not labels_subset:
        dbg("[RUN CHECK] No labels to check.")
        return

    dbg(f"[RUN CHECK] labels={list(labels_subset.keys())}")

    try:
        found_ids, found_users = await scrape_ws_task_ids()
        dbg(f"[RUN CHECK] scraped ids={len(found_ids)}")

        matched, missing = compare_logged_users(labels_subset, found_users)
        await send_report(title, matched, missing)

    except Exception as e:
        dbg(f"[RUN CHECK ERROR] {type(e).__name__}: {e}")
        try:
            await admin_client.send_message(REPORT_CHAT, f"❌ Checker error\n{type(e).__name__}: {e}")
        except Exception:
            pass

# =========================================================
# KNOWN LABELS
# =========================================================
def load_known_labels() -> Set[str]:
    data = load_json(KNOWN_LABELS_FILE, [])
    return set(data if isinstance(data, list) else [])

def save_known_labels(labels: Set[str]):
    save_json(KNOWN_LABELS_FILE, sorted(list(labels)))

# =========================================================
# LOOP 1
# =========================================================
async def new_label_watcher_loop():
    dbg("[LOOP] new_label_watcher_loop started")

    known_labels = load_known_labels()

    first_scan = await scan_all_labels()
    current_labels = set(first_scan.keys())

    if not known_labels:
        save_known_labels(current_labels)
        known_labels = current_labels
        dbg(f"[BOOT] Baseline labels saved: {len(known_labels)}")

    while True:
        try:
            label_map = await scan_all_labels()
            current_labels = set(label_map.keys())

            new_labels = current_labels - known_labels

            if new_labels:
                dbg(f"[NEW LABEL DETECTED] {new_labels}")

                known_labels = current_labels
                save_known_labels(known_labels)

                new_label_map = {lb: label_map[lb] for lb in new_labels if lb in label_map}

                await asyncio.sleep(NEW_LABEL_CHECK_DELAY)

                await run_check_for_labels(
                    new_label_map,
                    "🆕 NEW LABEL CHECK REPORT"
                )
            else:
                dbg("[SCAN] No new label")

            await asyncio.sleep(NEW_LABEL_SCAN_INTERVAL)

        except Exception as e:
            dbg(f"[WATCHER ERROR] {type(e).__name__}: {e}")
            await asyncio.sleep(10)

# =========================================================
# LOOP 2
# =========================================================
async def scheduled_recheck_loop():
    dbg("[LOOP] scheduled_recheck_loop started")

    while True:
        try:
            await asyncio.sleep(SCHEDULED_RECHECK_SECONDS)

            label_map = await scan_all_labels()
            if not label_map:
                dbg("[SCHEDULED] No labels found.")
                continue

            dbg(f"[SCHEDULED] Running full recheck for {len(label_map)} labels")

            await run_check_for_labels(
                label_map,
                "⏰ SCHEDULED WS CHECK REPORT"
            )

        except Exception as e:
            dbg(f"[SCHEDULED ERROR] {type(e).__name__}: {e}")
            await asyncio.sleep(30)

# =========================================================
# STARTUP
# =========================================================
async def startup_notice():
    try:
        await admin_client.send_message(
            REPORT_CHAT,
            "🤖 Refar checker started\n"
            f"⏱ New label scan: {NEW_LABEL_SCAN_INTERVAL}s\n"
            f"🔁 Recheck: {SCHEDULED_RECHECK_SECONDS}s\n"
            f"🎯 Match Source: label session logged user id\n"
            f"⏳ /teamactive reply wait enabled"
        )
    except Exception as e:
        dbg(f"[STARTUP NOTICE ERROR] {e}")

async def main():
    ensure_dir(DATA_DIR)

    if not acquire_lock():
        print("Another refar.py instance is already running. Exit.")
        return

    try:
        await admin_client.start()
        me = await admin_client.get_me()

        print(f"[+] Admin session started as @{getattr(me, 'username', None) or me.id}")
        print("[+] refar.py is running...")

        await startup_notice()

        await asyncio.gather(
            new_label_watcher_loop(),
            scheduled_recheck_loop(),
        )

    finally:
        release_lock()
        try:
            await admin_client.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())