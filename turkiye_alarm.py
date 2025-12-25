import os
import sqlite3
from datetime import datetime, timedelta, timezone
from math import radians, sin, cos, sqrt, atan2

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_FILE = "deprem.db"

def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik")
        return
    import requests
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = requests.post(url, data=payload, timeout=25)
    if r.status_code != 200:
        print("Telegram hata:", r.status_code, r.text)

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1, p2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dl = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(p1)*cos(p2)*sin(dl/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))

def get_rows_since_iso(since_iso: str):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    # DİKKAT: kolonlar latitude/longitude
    rows = cur.execute("""
        SELECT event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
    """, (since_iso,)).fetchall()
    con.close()
    return rows

def turkey_cluster_alarm(rows_30d, rows_7d, rows_24h, radius_km=100.0):
    """
    Türkiye geneli küme alarmı:
    🟠 (100km)
      - 24h: M>=3.0 >=40 ve maxMag>=4.0
      - 7d : M>=3.0 >=25 ve M>=4.0 >=2
      - 30d: M>=5.0 >=1
    🔴 (100km)
      - 7d : M>=6.5 >=1
      - 30d: M>=5.8 >=2
      - 24h: M>=4.0 >=10
    """
    def within(center_lat, center_lon, rows):
        return [r for r in rows if haversine_km(center_lat, center_lon, r[1], r[2]) <= radius_km]

    candidates = [(r[1], r[2]) for r in rows_30d if r[1] is not None and r[2] is not None]
    if not candidates:
        return {"orange": False, "red": False, "best": None}

    best = None
    for (clat, clon) in candidates[:250]:
        c24 = within(clat, clon, rows_24h)
        c7 = within(clat, clon, rows_7d)
        c30 = within(clat, clon, rows_30d)

        n24_m3 = sum(1 for r in c24 if r[4] >= 3.0)
        n24_m4 = sum(1 for r in c24 if r[4] >= 4.0)
        max24 = max([r[4] for r in c24], default=0.0)

        n7_m3 = sum(1 for r in c7 if r[4] >= 3.0)
        n7_m4 = sum(1 for r in c7 if r[4] >= 4.0)
        n7_m65 = sum(1 for r in c7 if r[4] >= 6.5)

        n30_m5 = sum(1 for r in c30 if r[4] >= 5.0)
        n30_m58 = sum(1 for r in c30 if r[4] >= 5.8)

        orange = (n24_m3 >= 40 and max24 >= 4.0) or (n7_m3 >= 25 and n7_m4 >= 2) or (n30_m5 >= 1)
        red = (n7_m65 >= 1) or (n30_m58 >= 2) or (n24_m4 >= 10)

        score = (2 if red else 1 if orange else 0) * 10_000 + n24_m3 * 50 + n7_m3 * 10 + n30_m5 * 100
        if best is None or score > best["score"]:
            best = {
                "score": score,
                "center": (clat, clon),
                "orange": orange,
                "red": red,
                "n24_m3": n24_m3,
                "n24_m4": n24_m4,
                "max24": max24,
                "n7_m3": n7_m3,
                "n7_m4": n7_m4,
                "n7_m65": n7_m65,
                "n30_m5": n30_m5,
                "n30_m58": n30_m58,
                "sample": c24[:5],
            }

    return {"orange": bool(best and best["orange"]), "red": bool(best and best["red"]), "best": best}

def main():
    now = datetime.now(timezone.utc)
    since_30 = (now - timedelta(days=30)).isoformat(timespec="seconds").replace("+00:00", "Z")
    since_7  = (now - timedelta(days=7)).isoformat(timespec="seconds").replace("+00:00", "Z")
    since_1  = (now - timedelta(days=1)).isoformat(timespec="seconds").replace("+00:00", "Z")

    rows_30d = get_rows_since_iso(since_30)
    rows_7d  = get_rows_since_iso(since_7)
    rows_24h = get_rows_since_iso(since_1)

    tr = turkey_cluster_alarm(rows_30d, rows_7d, rows_24h, radius_km=100.0)

    msg = []
    msg.append("🇹🇷 <b>TÜRKİYE KÜME ALARM (DB)</b>")
    msg.append(f"🕒 UTC: {now.isoformat(timespec='seconds').replace('+00:00','Z')}")
    msg.append(f"📦 DB 30g kayıt: {len(rows_30d)} | 7g: {len(rows_7d)} | 24h: {len(rows_24h)}")
    msg.append("")

    if tr["best"] is None:
        msg.append("• Küme hesaplanamadı / veri yok")
    else:
        b = tr["best"]
        clat, clon = b["center"]
        msg.append(f"• Küme merkezi: {clat:.4f}, {clon:.4f}")
        msg.append(f"• 24h: M≥3.0={b['n24_m3']} | M≥4.0={b['n24_m4']} | maxM={b['max24']:.1f}")
        msg.append(f"• 7g : M≥3.0={b['n7_m3']} | M≥4.0={b['n7_m4']} | M≥6.5={b['n7_m65']}")
        msg.append(f"• 30g: M≥5.0={b['n30_m5']} | M≥5.8={b['n30_m58']}")
        msg.append("")

        if b["red"]:
            msg.append("🔴 <b>KIRMIZI</b> (küme kriteri sağlandı)")
        elif b["orange"]:
            msg.append("🟠 <b>TURUNCU</b> (küme kriteri sağlandı)")
        else:
            msg.append("🟢 Normal (kriter yok)")

    # İstersen her saat mesaj at:
    telegram_send("\n".join(msg))
    print("Gönderildi (env varsa).")

if __name__ == "__main__":
    main()
