import os
import sqlite3
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from turkiye_alarm import turkey_alarm, within_radius, fetch_rows

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9700"))

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

# =========================
# Telegram
# =========================
def telegram_send(msg):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram ENV eksik")
        return
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
        timeout=20
    )

# =========================
# DB
# =========================
def init_db():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            latitude REAL,
            longitude REAL,
            depth REAL,
            magnitude REAL,
            location TEXT,
            source TEXT,
            UNIQUE(event_time, latitude, longitude, magnitude)
        )
    """)
    con.commit()
    con.close()

def insert_event(row):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO earthquakes
        (event_time, latitude, longitude, depth, magnitude, location, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, row)
    con.commit()
    inserted = cur.rowcount
    con.close()
    return inserted

# =========================
# KOERI
# =========================
def fetch_koeri():
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        return []

    rows = []
    for ln in pre.text.splitlines():
        if ln.strip() == "" or ln.startswith("Tarih"):
            continue
        try:
            p = ln.split()
            dt = datetime.strptime(f"{p[0]} {p[1]}", "%Y.%m.%d %H:%M:%S").isoformat()
            rows.append((
                dt,
                float(p[2]),
                float(p[3]),
                float(p[4]),
                float(p[6]),
                " ".join(p[8:]),
                "KOERI"
            ))
        except:
            continue
    return rows

# =========================
# MAIN
# =========================
def main():
    init_db()
    events = fetch_koeri()

    new = sum(insert_event(e) for e in events)
    print(f"KOERI satır: {len(events)} | Yeni eklenen: {new}")

    # Bandırma 70 km
    rows_30 = fetch_rows(30)
    bandirma = within_radius(rows_30, BANDIRMA_LAT, BANDIRMA_LON, 70)

    b_orange = any(r[4] >= 5.0 for r in bandirma)
    b_red    = any(r[4] >= 5.5 for r in bandirma)

    msg = "📡 <b>DEPREM DURUM RAPORU</b>\n\n"
    msg += "📍 <b>Bandırma (70 km)</b>\n"
    msg += f"🟠 Turuncu: {'AKTİF' if b_orange else 'YOK'}\n"
    msg += f"🔴 Kırmızı: {'AKTİF' if b_red else 'YOK'}\n\n"

    msg += turkey_alarm()

    telegram_send(msg)

if __name__ == "__main__":
    main()
