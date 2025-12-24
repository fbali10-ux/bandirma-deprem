import os
import sqlite3
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ===============================
# ENV (GitHub Actions Secrets)
# ===============================
BOT_TOKEN = os.getenv("8469619745:AAFv3Gbl5AFBvsHthRwfI8IrY24zOCv-Pyo")
CHAT_ID = os.getenv("-1003402835744")

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

# ===============================
# Telegram
# ===============================
def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    requests.post(url, data=payload, timeout=20)

# ===============================
# Database
# ===============================
def init_db():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            lat REAL,
            lon REAL,
            depth REAL,
            magnitude REAL,
            location TEXT,
            UNIQUE(event_time, lat, lon, magnitude)
        )
    """)
    con.commit()
    con.close()

def insert_event(row):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO earthquakes
            (event_time, lat, lon, depth, magnitude, location)
            VALUES (?, ?, ?, ?, ?, ?)
        """, row)
        con.commit()
        inserted = cur.rowcount
    except Exception as e:
        print("DB hata:", e)
        inserted = 0
    con.close()
    return inserted

# ===============================
# Fetch KOERI
# ===============================
def fetch_koeri():
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")

    pre = soup.find("pre")
    if not pre:
        return []

    lines = pre.get_text().split("\n")
    events = []

    for ln in lines:
        if ln.strip() == "" or ln.startswith("Tarih"):
            continue

        try:
            parts = ln.split()
            date = parts[0]
            time = parts[1]
            lat = float(parts[2])
            lon = float(parts[3])
            depth = float(parts[4])
            mag = float(parts[6])
            location = " ".join(parts[8:])

            event_time = datetime.strptime(
                f"{date} {time}", "%Y.%m.%d %H:%M:%S"
            ).isoformat()

            events.append((event_time, lat, lon, depth, mag, location))
        except:
            continue

    return events

# ===============================
# Alarm Kontrol (şimdilik basit)
# ===============================
def check_alarm(event):
    _, _, _, _, mag, _ = event
    return mag >= 4.5   # kriterle sonra oynayacağız

# ===============================
# MAIN
# ===============================
def main():
    init_db()
    events = fetch_koeri()

    new_events = []
    for ev in events:
        if insert_event(ev):
            new_events.append(ev)

    if not new_events:
        print("Yeni deprem yok")
        return

    alarm_events = [e for e in new_events if check_alarm(e)]

    if alarm_events:
        msg = "🚨 <b>DEPREM ALARM</b>\n\n"
        for e in alarm_events:
            t, lat, lon, d, m, loc = e
            msg += (
                f"📍 {loc}\n"
                f"🕒 {t}\n"
                f"🌍 {lat},{lon}\n"
                f"📏 Derinlik: {d} km\n"
                f"📊 Mw: <b>{m}</b>\n\n"
            )
        telegram_send(msg)
    else:
        print("Alarm yok")

if __name__ == "__main__":
    main()
