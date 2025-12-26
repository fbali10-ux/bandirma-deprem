import os
import sqlite3
import math
import requests
from datetime import datetime

# ------------------ AYARLAR ------------------
DB_FILE = "deprem.db"   # repo'daki gerçek DB adı buysa böyle kalsın. (earthquake.db ise değiştir)
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

def env_get(name, default=None):
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return v

def send_telegram(text: str):
    token = env_get("TELEGRAM_BOT_TOKEN")
    chat_id = env_get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=30)
    ok = (r.status_code == 200)
    if not ok:
        print("Telegram hata:", r.status_code, r.text[:200])
    return ok

# ------------------ DB ------------------
def connect_db(db_file: str):
    con = sqlite3.connect(db_file)
    con.execute("""
    CREATE TABLE IF NOT EXISTS quakes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qtime TEXT,
        lat REAL,
        lon REAL,
        depth REAL,
        mag REAL,
        loc TEXT
    )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_quakes_time ON quakes(qtime)")
    return con

def get_last5(con):
    cur = con.cursor()
    cur.execute("""
        SELECT qtime, mag, depth, lat, lon, loc
        FROM quakes
        ORDER BY qtime DESC
        LIMIT 5
    """)
    return cur.fetchall()

# ------------------ ALARM (şimdilik basit stub) ------------------
# Senin alarm/cluster fonksiyonların projede zaten varsa burada çağırırız.
# Şu an amaç: "son 5 deprem" kesin gitsin.
def check_alarm(con) -> (bool, str):
    # TODO: burada senin mevcut alarm/cluster mantığını çağıracağız.
    # Şimdilik "alarm yok" diyelim:
    return False, ""

def fmt_last5(rows):
    if not rows:
        return "DB boş görünüyor."
    lines = ["📌 Son 5 Deprem (KOERI DB)"]
    for qtime, mag, depth, lat, lon, loc in rows:
        # qtime string ise aynen bas
        lines.append(f"- {qtime} | M{mag:.1f} | {depth:.1f}km | {lat:.4f},{lon:.4f} | {loc}")
    return "\n".join(lines)

def main():
    con = connect_db(DB_FILE)

    # 1) HER ÇALIŞMADA SON 5 GÖNDER (kesin)
    last5 = get_last5(con)
    send_telegram(fmt_last5(last5))

    # 2) Alarm varsa ayrıca gönder
    has_alarm, alarm_msg = check_alarm(con)
    if has_alarm and alarm_msg:
        send_telegram(alarm_msg)

    con.close()

if __name__ == "__main__":
    main()
