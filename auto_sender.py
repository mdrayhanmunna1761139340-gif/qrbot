import asyncio
import random
import time
import config


def generate_number():
    prefix = random.choice(config.PREFIXES)
    min_len = max(config.MIN_LENGTH, len(prefix))
    max_len = max(config.MAX_LENGTH, len(prefix))
    total_len = random.randint(min_len, max_len)
    remaining = total_len - len(prefix)
    return prefix + "".join(random.choice("0123456789") for _ in range(remaining))


async def auto_sender_loop(
    user_id,
    label,
    client,
    session_running,
    auto_send_running,
    session_delay,
    next_send_time,
):
    if config.DEBUG:
        print(f"[AUTO SENDER STARTED] {user_id}:{label}")

    while True:
        try:
            if not session_running.get(user_id, {}).get(label, False):
                await asyncio.sleep(0.2)
                continue

            if not auto_send_running.get(user_id, {}).get(label, False):
                await asyncio.sleep(0.2)
                continue

            delay = session_delay[user_id].get(label, config.SEND_DELAY)

            if next_send_time[user_id].get(label, 0) <= 0:
                next_send_time[user_id][label] = time.monotonic() + delay
                if config.DEBUG:
                    print(f"[SCHEDULE SET] {user_id}:{label} -> {delay}s")

            now = time.monotonic()
            target_time = next_send_time[user_id][label]

            if now < target_time:
                await asyncio.sleep(min(target_time - now, 0.2))
                continue

            entity = await client.get_entity(config.TARGET_CHAT)
            number = generate_number()
            await client.send_message(entity, number)

            next_send_time[user_id][label] = target_time + delay

            if config.DEBUG:
                left = round(next_send_time[user_id][label] - time.monotonic(), 2)
                print(f"[AUTO SEND] {user_id}:{label} -> {number} | delay={delay}s | next_in={left}s")

        except Exception as e:
            print(f"[ERROR AUTO SEND] {user_id}:{label} -> {type(e).__name__}: {e}")
            await asyncio.sleep(1)