import sqlite3
import math
import os
from datetime import datetime, timedelta, timezone
import requests

# ===============================
# ENV
# ===============================
DB_FILE = "deprem.db"

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9767"))

# ===============================
# TELEGRAM
# ===============================
def send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(
        url,
        data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
        timeout=20
    )

# ===============================
# GEO
# ===============================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

# ===============================
# DB
# ===============================
def get_rows_since(days: int):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")

    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        SELECT event_time, latitude, longitude, magnitude
        FROM earthquakes
        WHERE event_time >= ?
    """, (since,))
    rows = cur.fetchall()
    con.close()
    return rows

# ===============================
# TÜRKİYE GENELİ ALARM
# ===============================
def turkey_cluster_alarm(rows):
    m30 = [r for r in rows if r[3] >= 5.0]
    m14 = [r for r in rows if r[3] >= 5.5]

    if len(m14) >= 1:
        return "🔴 <b>TÜRKİYE GENELİ KIRMIZI</b>\nMw≥5.5 (14 gün)"

    if len(m30) >= 1:
        return "🟠 <b>TÜRKİYE GENELİ TURUNCU</b>\nMw≥5.0 (30 gün)"

    return "🟢 <b>TÜRKİYE GENELİ NORMAL</b>"

# ===============================
# BANDIRMA 70 KM ALARM
# ===============================
def bandirma_alarm(rows):
    r70 = [
        r for r in rows
        if haversine(BANDIRMA_LAT, BANDIRMA_LON, r[1], r[2]) <= 70
    ]

    red = [r for r in r70 if r[3] >= 5.5]
    orange = [r for r in r70 if r[3] >= 5.0]

    if red:
        return f"🔴 <b>BANDIRMA 70 KM KIRMIZI</b>\nMw≥5.5 | Adet: {len(red)}"

    if orange:
        return f"🟠 <b>BANDIRMA 70 KM TURUNCU</b>\nMw≥5.0 | Adet: {len(orange)}"

    return "🟢 <b>BANDIRMA 70 KM NORMAL</b>"

# ===============================
# MAIN
# ===============================
def main():
    rows_30 = get_rows_since(30)
    rows_14 = get_rows_since(14)

    turkey_status = turkey_cluster_alarm(rows_30)
    bandirma_status = bandirma_alarm(rows_14)

    msg = (
        "📡 <b>DEPREM RİSK DEĞERLENDİRME</b>\n\n"
        f"{turkey_status}\n\n"
        f"{bandirma_status}\n\n"
        f"🕒 Güncelleme: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )

    send_telegram(msg)
    print("Alarm değerlendirmesi gönderildi")

if __name__ == "__main__":
    main()
