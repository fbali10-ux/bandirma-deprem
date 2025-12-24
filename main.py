# main.py
# KOERI -> SQLite -> analiz
# Windows 10 + Python 3.13 uyumlu
# 50.000 kayıt limiti, mükerrer imkansız

import sqlite3
import requests
import re
import math
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone

# ------------------ AYARLAR ------------------

KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"
DB_PATH = "deprem.db"
MAX_ROWS = 50_000

# Bandırma merkez (yaklaşık)
BANDIRMA_LAT = 40.3520
BANDIRMA_LON = 27.9700
BANDIRMA_RADIUS_KM = 100.0

# ------------------ YARDIMCI FONKSİYONLAR ------------------

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def row_hash(*vals):
    h = hashlib.sha256()
    h.update("|".join(map(str, vals)).encode("utf-8"))
    return h.hexdigest()

# ------------------ DB HAZIRLIK ------------------

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS earthquakes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time TEXT,
    lat REAL,
    lon REAL,
    depth_km REAL,
    magnitude REAL,
    location TEXT,
    source TEXT,
    hash TEXT UNIQUE
)
""")
conn.commit()

# ------------------ KOERI VERİ ÇEK ------------------

html = requests.get(KOERI_URL, timeout=30).content
soup = BeautifulSoup(html, "html.parser")
pre = soup.find("pre")

if not pre:
    print("KOERI sayfa formatı değişmiş olabilir.")
    exit(1)

lines = pre.get_text().splitlines()

pattern = re.compile(
    r"(?P<date>\d{4}\.\d{2}\.\d{2})\s+"
    r"(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<lat>-?\d+\.\d+)\s+"
    r"(?P<lon>-?\d+\.\d+)\s+"
    r"(?P<depth>\d+\.\d+)\s+"
    r"(?P<mag>\d+\.\d+)\s+.*?\s+"
    r"(?P<loc>.+)"
)

inserted = 0

for ln in lines:
    m = pattern.search(ln)
    if not m:
        continue

    dt_str = f"{m.group('date')} {m.group('time')}"
    dt = datetime.strptime(dt_str, "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)

    lat = float(m.group("lat"))
    lon = float(m.group("lon"))
    depth = float(m.group("depth"))
    mag = float(m.group("mag"))
    loc = m.group("loc").strip()

    h = row_hash(dt.isoformat(), lat, lon, depth, mag, loc)

    try:
        cur.execute("""
            INSERT INTO earthquakes
            (event_time, lat, lon, depth_km, magnitude, location, source, hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            dt.isoformat(),
            lat, lon, depth, mag,
            loc, "KOERI", h
        ))
        inserted += 1
    except sqlite3.IntegrityError:
        pass

conn.commit()

# ------------------ 50.000 SATIR LİMİTİ ------------------

cur.execute("SELECT COUNT(*) FROM earthquakes")
total_rows = cur.fetchone()[0]

if total_rows > MAX_ROWS:
    delete_count = total_rows - MAX_ROWS
    cur.execute("""
        DELETE FROM earthquakes
        WHERE id IN (
            SELECT id FROM earthquakes
            ORDER BY event_time ASC
            LIMIT ?
        )
    """, (delete_count,))
    conn.commit()

# ------------------ BANDIRMA ANALİZ ------------------

since_48h = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()

cur.execute("""
    SELECT event_time, lat, lon, magnitude, location
    FROM earthquakes
    WHERE event_time >= ?
""", (since_48h,))

rows = cur.fetchall()

bandirma_events = []

for et, lat, lon, mag, loc in rows:
    d = haversine_km(BANDIRMA_LAT, BANDIRMA_LON, lat, lon)
    if d <= BANDIRMA_RADIUS_KM:
        bandirma_events.append((et, mag, d, loc))

bandirma_events.sort(key=lambda x: x[0], reverse=True)

# ------------------ TÜRKİYE GENELİ KÜME ------------------

cluster = [r for r in rows if r[3] >= 4.0]

# ------------------ RAPOR ------------------

print("\n--- Bandırma 100 km Analizi ---")
print(f"Toplam yerel kayıt (son 48 saat): {len(bandirma_events)}")

if not bandirma_events:
    print("Alarm seviyesi: YOK")
else:
    max_mag = max(e[1] for e in bandirma_events)
    print(f"En büyük deprem: M{max_mag:.1f}")

print("\nSon 48 saatteki en büyük 5 yerel kayıt:")
for e in bandirma_events[:5]:
    print(f"- {e[0]} | M{e[1]:.1f} | {e[2]:.1f} km | {e[3]}")

print("\n--- Türkiye Geneli Kümeleşme (basit) ---")
if len(cluster) >= 5:
    print(f"DİKKAT: Son 48 saatte {len(cluster)} adet M>=4.0 deprem.")
else:
    print("Son 48 saatte (M>=4.0) belirgin kümeleşme yok.")

print("\nToplam DB kayıt sayısı:", min(total_rows, MAX_ROWS))
print("Yeni eklenen kayıt:", inserted)

conn.close()
def telegram_test():
    import os
    import requests

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("Telegram env eksik")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "✅ Bandırma Deprem Alarm sistemi çalışıyor (TEST)"
    }

    r = requests.post(url, json=payload, timeout=30)
    print("Telegram response:", r.text)


telegram_test()

