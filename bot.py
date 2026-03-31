# -*- coding: utf-8 -*-
import os
import sys
import shutil
import subprocess
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

BOT_FILE = os.path.join(BASE_DIR, "oo.py")
REFAR_FILE = os.path.join(BASE_DIR, "refar.py")


def delete_data_folder():
    """
    Bot run হওয়ার আগে একবার শুধু data folder delete করবে
    """
    if os.path.exists(DATA_DIR) and os.path.isdir(DATA_DIR):
        try:
            shutil.rmtree(DATA_DIR)
            print(f"[OK] Deleted data folder: {DATA_DIR}")
        except Exception as e:
            print(f"[ERROR] Failed to delete data folder -> {e}")
    else:
        print("[INFO] data folder not found, skip delete.")


def start_process(py_file, name):
    """
    আলাদা process এ py file start করবে
    """
    if not os.path.exists(py_file):
        print(f"[ERROR] {name} file not found: {py_file}")
        return None

    try:
        proc = subprocess.Popen([sys.executable, py_file])
        print(f"[STARTED] {name} | PID={proc.pid}")
        return proc
    except Exception as e:
        print(f"[ERROR] Failed to start {name} -> {e}")
        return None


def main():
    print("=" * 50)
    print("RUNNER STARTING...")
    print("=" * 50)

    # 1) প্রথমে data folder delete
    delete_data_folder()

    # 2) bot.py + refar.py একসাথে start
    bot_proc = start_process(BOT_FILE, "bot.py")
    refar_proc = start_process(REFAR_FILE, "refar.py")

    if not bot_proc and not refar_proc:
        print("[FATAL] Nothing started.")
        return

    print("\n[OK] All processes started.")
    print("Press CTRL+C to stop all.\n")

    try:
        while True:
            time.sleep(2)

            # শুধু status দেখবে, restart করবে না
            if bot_proc and bot_proc.poll() is not None:
                print("[STOPPED] bot.py exited.")

            if refar_proc and refar_proc.poll() is not None:
                print("[STOPPED] refar.py exited.")

            # যদি দুইটাই বন্ধ হয়ে যায়, loop break
            if ((bot_proc is None or bot_proc.poll() is not None) and
                (refar_proc is None or refar_proc.poll() is not None)):
                print("[EXIT] All processes stopped.")
                break

    except KeyboardInterrupt:
        print("\n[CTRL+C] Stopping all processes...")

        for proc, name in [(bot_proc, "bot.py"), (refar_proc, "refar.py")]:
            try:
                if proc and proc.poll() is None:
                    proc.terminate()
                    print(f"[TERMINATED] {name}")
            except Exception:
                pass

        time.sleep(2)

        for proc, name in [(bot_proc, "bot.py"), (refar_proc, "refar.py")]:
            try:
                if proc and proc.poll() is None:
                    proc.kill()
                    print(f"[KILLED] {name}")
            except Exception:
                pass

        print("[DONE] All stopped.")


if __name__ == "__main__":
    main()