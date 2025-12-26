import os
import re
import math
import sqlite3
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

# --------- Ayarlar ----------
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9700"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "100"))  # istersen repo variable ekleyebilirsin
MAX_FETCH = int(os.getenv("MAX_FETCH", "500"))  # KOERI'den parse edilen satır limiti
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
    a = math.sin(dlat/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def clean_place(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def parse_mag(s: str):
    try:
        return float(str(s).replace(",", "."))
    except Exception:
        return None

def to_iso_utc(dt_local_str: str):
    """
    KOERI formatı genelde: 'YYYY.MM.DD HH:MM:SS'
    KOERI saatinin TR (UTC+3) olduğu varsayımıyla UTC'ye çevirip ISO döndürür.
    """
    dt_local = datetime.strptime(dt_local_str.strip(), "%Y.%m.%d %H:%M:%S")
    # TR = UTC+3
    dt_aware = dt_local.replace(tzinfo=timezone.utc)  # önce "naive"ı UTC kabul etmeyelim
    # doğru dönüşüm: TR(+3) -> UTC : 3 saat çıkar
    dt_utc = (dt_local - (datetime(1970,1,1) - datetime(1970,1,1))).replace(tzinfo=None)  # no-op
    dt_utc = dt_local.replace(tzinfo=timezone.utc)  # placeholder değil, aşağıda net yapacağız

    # net: dt_local = UTC+3, UTC = dt_local - 3h
    dt_utc = (dt_local - timedelta_hours(3)).replace(tzinfo=timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")

def timedelta_hours(h):
    from datetime import timedelta
    return timedelta(hours=h)

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Mesaj atlanıyor.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)
    ok = (r.status_code == 200)
    if not ok:
        print("Telegram gönderim hatası:", r.status_code, r.text[:300])
    return ok

# --------- DB ----------
def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS quakes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dt TEXT NOT NULL,
        lat REAL,
        lon REAL,
        depth_km REAL,
        mag REAL,
        place TEXT,
        source TEXT DEFAULT 'KOERI',
        UNIQUE(dt, lat, lon, mag)
    )
    """)
    conn.commit()

def upsert_quakes(conn: sqlite3.Connection, rows):
    """
    rows: list of (dt_iso_utc, lat, lon, depth, mag, place, source)
    """
    added = 0
    for row in rows:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO quakes(dt, lat, lon, depth_km, mag, place, source)
                VALUES(?,?,?,?,?,?,?)
            """, row)
            if conn.total_changes > 0:
                added += 1
        except Exception:
            pass
    conn.commit()
    return added

def get_last_quakes(conn: sqlite3.Connection, n=10):
    cur = conn.execute("""
        SELECT dt, mag, place, lat, lon
        FROM quakes
        ORDER BY dt DESC
        LIMIT ?
    """, (n,))
    return cur.fetchall()

def get_last_bandirma(conn: sqlite3.Connection, n=10):
    # Bandırma yarıçap filtresi python tarafında yapılacak (basit/okunur)
    all_last = get_last_quakes(conn, 200)  # bandırma için yeterince geniş al
    out = []
    for dt, mag, place, lat, lon in all_last:
        if lat is None or lon is None:
            continue
        d = haversine_km(BANDIRMA_LAT, BANDIRMA_LON, float(lat), float(lon))
        if d <= BANDIRMA_RADIUS_KM:
            out.append((dt, mag, place, d))
            if len(out) >= n:
                break
    return out

# --------- KOERI Parse ----------
def fetch_koeri():
    html = requests.get(KOERI_URL, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı. Format değişmiş olabilir.")
    text = pre.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    # Başlık satırlarını ele
    # Veri satırları tipik: 2025.12.26 18:15:52  39.1234  27.1234  5.2  2.7  YER ...
    data = []
    for ln in lines:
        if re.match(r"^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}", ln):
            data.append(ln)
    return data[:MAX_FETCH]

def parse_koeri_lines(data_lines):
    out = []
    for ln in data_lines:
        parts = re.split(r"\s+", ln)
        # beklenen minimum kolonlar: date time lat lon depth mag place...
        if len(parts) < 7:
            continue
        dt_local_str = f"{parts[0]} {parts[1]}"
        lat = parse_mag(parts[2])
        lon = parse_mag(parts[3])
        depth = parse_mag(parts[4])
        mag = parse_mag(parts[5])
        place = clean_place(" ".join(parts[6:]))
        if not mag or lat is None or lon is None:
            continue
        # UTC ISO
        dt_utc = (datetime.strptime(dt_local_str, "%Y.%m.%d %H:%M:%S") - timedelta_hours(3)).replace(tzinfo=timezone.utc)
        dt_iso = dt_utc.isoformat().replace("+00:00", "Z")
        out.append((dt_iso, lat, lon, depth, mag, place, "KOERI"))
    return out

# --------- Alarm Mantığı (Basit ve Net) ----------
def alarm_level_turkey(last_quakes):
    """
    last_quakes: list of (dt, mag, place, lat, lon)
    Basit kural:
      - Son N içinde M>=6.0 -> TURUNCU
      - Son N içinde M>=6.5 -> KIRMIZI
    """
    level = "YOK"
    trigger = None
    for dt, mag, place, lat, lon in last_quakes:
        if mag is None:
            continue
        if mag >= 6.5:
            return "KIRMIZI", (dt, mag, place)
        if mag >= 6.0 and level != "KIRMIZI":
            level = "TURUNCU"
            trigger = (dt, mag, place)
    return level, trigger

def alarm_level_bandirma(last_bandirma):
    """
    last_bandirma: list of (dt, mag, place, distance_km)
    Basit kural (Bandırma 100km):
      - M>=5.0 -> TURUNCU
      - M>=5.5 -> KIRMIZI
    """
    level = "YOK"
    trigger = None
    for dt, mag, place, dkm in last_bandirma:
        if mag is None:
            continue
        if mag >= 5.5:
            return "KIRMIZI", (dt, mag, place, dkm)
        if mag >= 5.0 and level != "KIRMIZI":
            level = "TURUNCU"
            trigger = (dt, mag, place, dkm)
    return level, trigger

def format_dt_hhmm(dt_iso):
    # dt_iso: "2025-12-26T18:15:52Z"
    try:
        dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
        return dt.strftime("%H:%M")
    except Exception:
        return dt_iso

def build_message(last10, band10):
    t_level, t_trig = alarm_level_turkey(last10)
    b_level, b_trig = alarm_level_bandirma(band10)

    lines = []
    lines.append("📡 Deprem Durumu (otomatik)")
    lines.append(f"🇹🇷 Türkiye geneli: {t_level if t_level!='YOK' else 'ALARM YOK'}")
    if t_trig:
        dt, mag, place = t_trig
        lines.append(f"   ↳ Tetik: {format_dt_hhmm(dt)} | M{mag:.1f} | {place}")

    lines.append(f"📍 Bandırma {int(BANDIRMA_RADIUS_KM)} km: {b_level if b_level!='YOK' else 'ALARM YOK'}")
    if b_trig:
        dt, mag, place, dkm = b_trig
        lines.append(f"   ↳ Tetik: {format_dt_hhmm(dt)} | M{mag:.1f} | {place} | ~{dkm:.1f} km")

    lines.append("")
    lines.append(f"🕒 Son {LAST_N} Deprem (Türkiye):")
    if not last10:
        lines.append(" - Kayıt yok (DB boş).")
    else:
        for dt, mag, place, lat, lon in last10[:LAST_N]:
            hhmm = format_dt_hhmm(dt)
            mtxt = f"M{mag:.1f}" if mag is not None else "M?"
            lines.append(f" - {hhmm} | {mtxt} | {place}")

    return "\n".join(lines)

def main():
    # 1) KOERI çek + DB güncelle
    koeri_lines = fetch_koeri()
    parsed = parse_koeri_lines(koeri_lines)

    conn = sqlite3.connect(DB_FILE)
    try:
        init_db(conn)
        added = upsert_quakes(conn, parsed)
        print(f"KOERI satır: {len(parsed)} | Yeni eklenen: {added}")

        # 2) DB'den son kayıtlar
        last10 = get_last_quakes(conn, LAST_N)
        band10 = get_last_bandirma(conn, LAST_N)

        # 3) Telegram’a HER SEFERİNDE mesaj
        msg = build_message(last10, band10)
        send_telegram(msg)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
