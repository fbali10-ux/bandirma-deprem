import os
import re
import math
import sqlite3
import hashlib
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst6.asp"

# --------- Ayarlar ----------
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9700"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "100"))
MAX_FETCH = int(os.getenv("MAX_FETCH", "500"))
LAST_N = int(os.getenv("LAST_N", "10"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# --------- Yardımcılar ----------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\uFFFD", " ")  # � karakteri
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def try_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def koeri_local_to_utc_iso(dt_local_str: str) -> str:
    # KOERI saatini TR (UTC+3) kabul edip UTC'ye çeviriyoruz
    dt_local = datetime.strptime(dt_local_str.strip(), "%Y.%m.%d %H:%M:%S")
    dt_utc = (dt_local - timedelta(hours=3)).replace(tzinfo=timezone.utc)
    return dt_utc.isoformat()

def make_event_id(event_time_iso: str, lat: float, lon: float, mag: float) -> str:
    key = f"{event_time_iso}|{lat:.4f}|{lon:.4f}|{mag:.2f}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Mesaj atlanıyor.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)
    ok = (r.status_code == 200)
    if not ok:
        print("Telegram gönderim hatası:", r.status_code, r.text[:300])
    return ok

# --------- DB (earthquakes tablosu) ----------
def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS earthquakes (
        event_id TEXT PRIMARY KEY,
        event_time TEXT NOT NULL,   -- ISO UTC
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        depth REAL,
        magnitude REAL,
        location TEXT,
        source TEXT
    )
    """)
    conn.commit()

def upsert_earthquakes(conn: sqlite3.Connection, rows):
    """
    rows: list of (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
    """
    cur = conn.cursor()
    before = conn.total_changes
    cur.executemany("""
        INSERT OR IGNORE INTO earthquakes
        (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    return conn.total_changes - before

def get_last_quakes(conn: sqlite3.Connection, n: int):
    cur = conn.execute("""
        SELECT event_time, magnitude, location, latitude, longitude
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT ?
    """, (n,))
    return cur.fetchall()

def get_last_bandirma(conn: sqlite3.Connection, n: int):
    # Python tarafında mesafe filtresi
    all_last = get_last_quakes(conn, 300)
    out = []
    for event_time, mag, loc, lat, lon in all_last:
        if lat is None or lon is None:
            continue
        d = haversine_km(BANDIRMA_LAT, BANDIRMA_LON, float(lat), float(lon))
        if d <= BANDIRMA_RADIUS_KM:
            out.append((event_time, mag, loc, d))
            if len(out) >= n:
                break
    return out

# --------- KOERI Parse ----------
def fetch_koeri_lines():
    html = requests.get(KOERI_URL, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı. Format değişmiş olabilir.")
    text = pre.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    data = []
    for ln in lines:
        if re.match(r"^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}", ln):
            data.append(ln)
    return data[:MAX_FETCH]

def parse_koeri_line(ln: str):
    """
    Örnek:
    2025.12.26 23:36:44 39.1427 28.1030 5.0 -.- 1.5 -.- SINANDEDE-SINDIRGI (BALIKESIR) İlksel

    Biz şunu yapıyoruz:
    - dt, lat, lon, depth sabit yerlerden
    - kalan kısımda sayısal değerlerden "son görünen float" -> magnitude
    - magnitude'dan sonrası -> location (+ en sonda İlksel/Revize vb varsa source)
    """
    parts = re.split(r"\s+", ln)
    if len(parts) < 7:
        return None

    dt_local_str = f"{parts[0]} {parts[1]}"
    lat = try_float(parts[2])
    lon = try_float(parts[3])
    depth = try_float(parts[4])

    if lat is None or lon is None:
        return None

    tail = parts[5:]

    # tail içindeki float tokenları bul
    float_idxs = []
    float_vals = []
    for i, tok in enumerate(tail):
        v = try_float(tok)
        if v is not None:
            float_idxs.append(i)
            float_vals.append(v)

    if not float_vals:
        return None

    # magnitude olarak "tail içindeki son float"ı al (KOERI'de genelde ML/Mw sütunları var)
    mag_idx = float_idxs[-1]
    mag = float_vals[-1]
    if mag is None or mag <= 0:
        return None

    # mag'dan sonraki tokenlar location + source
    after = tail[mag_idx + 1:]
    after = [clean_text(x) for x in after if clean_text(x)]

    source = "KOERI"
    if after:
        last = after[-1].lower()
        # KOERI'de sonda "İlksel/Revize" gibi durumlar oluyor
        if last in {"ilksel", "i̇lksel", "revize", "revised", "lokal"}:
            source = after[-1]
            after = after[:-1]

    location = clean_text(" ".join(after))
    event_time = koeri_local_to_utc_iso(dt_local_str)
    event_id = make_event_id(event_time, float(lat), float(lon), float(mag))

    return (event_id, event_time, float(lat), float(lon), depth, float(mag), location, source)

def parse_koeri_lines(lines):
    out = []
    for ln in lines:
        row = parse_koeri_line(ln)
        if row:
            out.append(row)
    return out

# --------- Alarm Mantığı ----------
def alarm_level_turkey(last_quakes):
    # last_quakes: (event_time, magnitude, location, lat, lon)
    level = "YOK"
    trigger = None
    for t, m, loc, lat, lon in last_quakes:
        if m is None:
            continue
        if m >= 6.5:
            return "KIRMIZI", (t, m, loc)
        if m >= 6.0 and level != "KIRMIZI":
            level = "TURUNCU"
            trigger = (t, m, loc)
    return level, trigger

def alarm_level_bandirma(last_bandirma):
    # last_bandirma: (event_time, magnitude, location, distance_km)
    level = "YOK"
    trigger = None
    for t, m, loc, dkm in last_bandirma:
        if m is None:
            continue
        if m >= 5.5:
            return "KIRMIZI", (t, m, loc, dkm)
        if m >= 5.0 and level != "KIRMIZI":
            level = "TURUNCU"
            trigger = (t, m, loc, dkm)
    return level, trigger

def fmt_hhmm(event_time_iso: str):
    try:
        dt = datetime.fromisoformat(event_time_iso)
        # DB UTC saklıyor; Telegram için TR saati istersen +3 ekleyebilirsin.
        # Şimdilik UTC saatini yazıyoruz.
        return dt.strftime("%H:%M")
    except Exception:
        return event_time_iso

def build_message(last10, band10):
    t_level, t_trig = alarm_level_turkey(last10)
    b_level, b_trig = alarm_level_bandirma(band10)

    lines = []
    lines.append("📡 Deprem Durumu (otomatik)")
    lines.append(f"🇹🇷 Türkiye geneli: {'ALARM YOK' if t_level=='YOK' else t_level}")
    if t_trig:
        t, m, loc = t_trig
        loc = loc if loc else "(yer bilgisi yok)"
        lines.append(f"   ↳ Tetik: {fmt_hhmm(t)} | M{m:.1f} | {loc}")

    lines.append(f"📍 Bandırma {int(BANDIRMA_RADIUS_KM)} km: {'ALARM YOK' if b_level=='YOK' else b_level}")
    if b_trig:
        t, m, loc, dkm = b_trig
        loc = loc if loc else "(yer bilgisi yok)"
        lines.append(f"   ↳ Tetik: {fmt_hhmm(t)} | M{m:.1f} | {loc} | ~{dkm:.1f} km")

    lines.append("")
    lines.append(f"🕒 Son {LAST_N} Deprem (Türkiye):")
    if not last10:
        lines.append(" - Kayıt yok (DB boş).")
    else:
        for t, m, loc, lat, lon in last10[:LAST_N]:
            loc = loc if loc else "(yer bilgisi yok)"
            mtxt = f"M{m:.1f}" if m is not None else "M?"
            lines.append(f" - {fmt_hhmm(t)} | {mtxt} | {loc}")

    return "\n".join(lines)

def main():
    koeri_lines = fetch_koeri_lines()
    parsed = parse_koeri_lines(koeri_lines)

    conn = sqlite3.connect(DB_FILE)
    try:
        init_db(conn)
        added = upsert_earthquakes(conn, parsed)
        print(f"KOERI parse: {len(parsed)} | Yeni eklenen: {added}")

        last10 = get_last_quakes(conn, LAST_N)
        band10 = get_last_bandirma(conn, LAST_N)

        msg = build_message(last10, band10)
        send_telegram(msg)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
