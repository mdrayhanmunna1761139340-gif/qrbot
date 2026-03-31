# -- coding: utf-8 --
import asyncio
from telethon import TelegramClient

API_ID = 35324324
API_HASH = "9964384cb72bb739302d2889998e713c"

SESSION_NAME = "admin"   # এটা admin.session বানাবে

async def main():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    await client.start()

    me = await client.get_me()
    print("====================================")
    print("✅ Login Success")
    print(f"Name: {me.first_name}")
    print(f"Username: @{me.username}" if me.username else "Username: None")
    print(f"User ID: {me.id}")
    print("====================================")
    print("admin.session created successfully!")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())