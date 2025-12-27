import os
import re
import math
import sqlite3
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

# -------------------- AYARLAR --------------------
KOERI_URL = os.getenv("KOERI_URL", "http://www.koeri.boun.edu.tr/scripts/lst6.asp")
DB_PATH = os.getenv("DB_PATH", "deprem.db")  # yerelde deprem_local.db yapacağız
MAX_ROWS = int(os.getenv("MAX_ROWS", "50000"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FORCE_TELEGRAM = os.getenv("FORCE_TELEGRAM", "0") == "1"

# Bandırma merkez (yaklaşık)
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9767"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "100"))
BANDIRMA_LIST_N = int(os.getenv("BANDIRMA_LIST_N", "5"))

# Alarm eşikleri (şimdilik basit)
ORANGE_MW = float(os.getenv("ORANGE_MW", "5.0"))
RED_MW = float(os.getenv("RED_MW", "6.0"))

# -------------------- YARDIMCILAR --------------------
def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*r*math.asin(math.sqrt(a))

def ensure_columns(conn):
    cur = conn.cursor()

    # Tablo yoksa oluştur
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes (
            event_time TEXT,
            latitude REAL,
            longitude REAL,
            magnitude REAL,
            depth_km REAL,
            location TEXT
        )
    """)

    # Var olan kolonlar
    cur.execute("PRAGMA table_info(earthquakes)")
    cols = {row[1] for row in cur.fetchall()}

    # Eksikleri ekle (eski db uyumu)
    if "event_time" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN event_time TEXT")
    if "latitude" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN latitude REAL")
    if "longitude" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN longitude REAL")
    if "magnitude" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN magnitude REAL")
    if "depth_km" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN depth_km REAL")
    if "location" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN location TEXT")

    # Mükerrer engelle
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_eq
        ON earthquakes(event_time, latitude, longitude, magnitude, depth_km, location)
    """)
    conn.commit()

def trim_db(conn, max_rows=50000):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM earthquakes")
    n = cur.fetchone()[0]
    if n <= max_rows:
        return
    to_delete = n - max_rows
    cur.execute("""
        DELETE FROM earthquakes
        WHERE rowid IN (
            SELECT rowid FROM earthquakes
            ORDER BY event_time ASC
            LIMIT ?
        )
    """, (to_delete,))
    conn.commit()

def parse_koeri():
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı (format değişmiş olabilir).")

    lines = [ln.strip() for ln in pre.get_text("\n").splitlines() if ln.strip()]
    data_lines = []
    for ln in lines:
        if ln.startswith("Tarih") or ln.startswith("------"):
            continue
        data_lines.append(ln)

    rows = []
    for ln in data_lines[:500]:
        parts = re.split(r"\s+", ln)
        if len(parts) < 8:
            continue

        dt_str = parts[0] + " " + parts[1]
        try:
            dt = datetime.strptime(dt_str, "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        try:
            lat = float(parts[2])
            lon = float(parts[3])
            depth = float(parts[4])
        except Exception:
            continue

        mag = None
        mag_idx = None
        for idx in range(5, min(9, len(parts))):
            try:
                mag = float(parts[idx])
                mag_idx = idx
                break
            except Exception:
                pass
        if mag is None:
            continue

        loc_start = (mag_idx + 1) if mag_idx is not None else 8
        location = " ".join(parts[loc_start:]).strip()
        if not location:
            location = "-"

        rows.append({
            "event_time": dt.isoformat(),
            "latitude": lat,
            "longitude": lon,
            "depth_km": depth,
            "magnitude": mag,
            "location": location
        })

    return rows

def upsert_rows(conn, rows):
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        cur.execute("""
            INSERT OR IGNORE INTO earthquakes(event_time, latitude, longitude, magnitude, depth_km, location)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (r["event_time"], r["latitude"], r["longitude"], r["magnitude"], r["depth_km"], r["location"]))
        if cur.rowcount == 1:
            inserted += 1
    conn.commit()
    return inserted

def get_bandirma_last_n(conn, n=5):
    cur = conn.cursor()
    cur.execute("""
        SELECT event_time, latitude, longitude, depth_km, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT 500
    """)
    cand = cur.fetchall()
    picked = []
    for et, lat, lon, depth, mag, loc in cand:
        d = haversine_km(BANDIRMA_LAT, BANDIRMA_LON, lat, lon)
        if d <= BANDIRMA_RADIUS_KM:
            picked.append((et, lat, lon, depth, mag, loc, d))
        if len(picked) >= n:
            break
    return picked

def compute_alarm_status(bandirma_events):
    if not bandirma_events:
        return "NORMAL", None
    max_mag = max(ev[4] for ev in bandirma_events)
    if max_mag >= RED_MW:
        return "RED", max_mag
    if max_mag >= ORANGE_MW:
        return "ORANGE", max_mag
    return "NORMAL", max_mag

def build_message(status, max_mag, bandirma_events, inserted):
    now_tr = datetime.now().strftime("%d.%m.%Y %H:%M")
    head = f"📍 *Bandırma Deprem Alarm*\n🕒 {now_tr}\n"
    head += f"🧠 Alarm: *{status}*"
    if max_mag is not None:
        head += f" (max Mw={max_mag:.1f})"
    head += "\n"
    head += f"➕ Yeni eklenen kayıt: *{inserted}*\n\n"

    if not bandirma_events:
        return head + f"Bandırma {int(BANDIRMA_RADIUS_KM)} km içinde kayıt bulunamadı."

    lines = [head + f"📌 Bandırma {int(BANDIRMA_RADIUS_KM)} km içi *son {len(bandirma_events)} deprem*:\n"]
    for et, lat, lon, depth, mag, loc, dist in bandirma_events:
        try:
            dt = datetime.fromisoformat(et.replace("Z", "+00:00"))
            et_out = dt.strftime("%d.%m %H:%M")
        except Exception:
            et_out = et[:16]

        lines.append(
            f"• *{mag:.1f}* | {et_out} | {depth:.1f} km | {dist:.0f} km\n  {loc}"
        )

    return "\n".join(lines)

def telegram_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Mesaj atlanıyor.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }, timeout=20)
        if r.status_code != 200:
            print("Telegram gönderim hatası:", r.status_code, r.text[:300])
            return False
        return True
    except Exception as e:
        print("Telegram gönderim exception:", e)
        return False

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)

    rows = parse_koeri()
    inserted = upsert_rows(conn, rows)
    trim_db(conn, MAX_ROWS)

    bandirma_events = get_bandirma_last_n(conn, BANDIRMA_LIST_N)
    status, max_mag = compute_alarm_status(bandirma_events)

    print(f"KOERI parse: {min(500, len(rows))} | Yeni eklenen: {inserted}")

    send = FORCE_TELEGRAM or inserted > 0 or status in ("ORANGE", "RED")
    if send:
        msg = build_message(status, max_mag, bandirma_events, inserted)
        telegram_send(msg)

    conn.close()

if __name__ == "__main__":
    main()
