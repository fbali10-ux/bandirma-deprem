import sqlite3
import math
from datetime import datetime, timedelta

DB_FILE = "deprem.db"

# =========================
# Yardımcılar
# =========================
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def fetch_rows(days):
    since = (datetime.utcnow() - timedelta(days=days)).isoformat(timespec="seconds")
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        SELECT event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
    """, (since,))
    rows = cur.fetchall()
    con.close()
    return rows

def within_radius(rows, clat, clon, radius_km):
    return [
        r for r in rows
        if haversine_km(clat, clon, r[1], r[2]) <= radius_km
    ]

# =========================
# TÜRKİYE GENELİ ALARMI
# =========================
def turkey_alarm():
    rows_24h = fetch_rows(1)
    rows_7d  = fetch_rows(7)
    rows_30d = fetch_rows(30)

    m3_24 = sum(1 for r in rows_24h if r[4] >= 3.0)
    m4_24 = sum(1 for r in rows_24h if r[4] >= 4.0)

    m3_7  = sum(1 for r in rows_7d if r[4] >= 3.0)
    m4_7  = sum(1 for r in rows_7d if r[4] >= 4.0)
    m65_7 = sum(1 for r in rows_7d if r[4] >= 6.5)

    m5_30  = sum(1 for r in rows_30d if r[4] >= 5.0)
    m58_30 = sum(1 for r in rows_30d if r[4] >= 5.8)

    orange = (
        (m3_24 >= 40 and max([r[4] for r in rows_24h], default=0) >= 4.0) or
        (m3_7 >= 25 and m4_7 >= 2) or
        (m5_30 >= 1)
    )

    red = (
        (m65_7 >= 1) or
        (m58_30 >= 2) or
        (m4_24 >= 10)
    )

    msg = "🇹🇷 <b>TÜRKİYE GENELİ DEPREM DURUMU</b>\n"
    msg += f"🟠 Turuncu: {'AKTİF' if orange else 'YOK'}\n"
    msg += f"🔴 Kırmızı: {'AKTİF' if red else 'YOK'}\n"

    return msg
