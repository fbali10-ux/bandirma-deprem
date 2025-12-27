import os
import re
import math
import sqlite3
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

# -------------------- AYARLAR --------------------
KOERI_URL = os.getenv("KOERI_URL", "http://www.koeri.boun.edu.tr/scripts/lst6.asp")

# Yerelde: deprem_local.db kullan. Bulutta (GitHub Actions): deprem.db kalsın.
DB_PATH = os.getenv("DB_PATH", "deprem.db")

# DB toplam kapasite (varsayılan 50.000 kayıt)
MAX_ROWS = int(os.getenv("MAX_ROWS", "50000"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FORCE_TELEGRAM = os.getenv("FORCE_TELEGRAM", "0") == "1"

# Alarm eşikleri (merkezler için)
ORANGE_MW = float(os.getenv("ORANGE_MW", "5.0"))
RED_MW = float(os.getenv("RED_MW", "6.0"))

# Türkiye geneli eşikler (istersen ayrı)
TR_ORANGE_MW = float(os.getenv("TR_ORANGE_MW", str(ORANGE_MW)))
TR_RED_MW = float(os.getenv("TR_RED_MW", str(RED_MW)))

# Türkiye alarmı hangi pencereden bakacak? (son N kayıt)
TR_WINDOW_N = int(os.getenv("TR_WINDOW_N", "500"))

# --------- Bandırma merkez ---------
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9767"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "100"))
BANDIRMA_LIST_N = int(os.getenv("BANDIRMA_LIST_N", "5"))

# --------- Bursa merkez ---------
BURSA_LAT = float(os.getenv("BURSA_LAT", "40.1950"))
BURSA_LON = float(os.getenv("BURSA_LON", "29.0600"))
BURSA_RADIUS_KM = float(os.getenv("BURSA_RADIUS_KM", "100"))
BURSA_LIST_N = int(os.getenv("BURSA_LIST_N", "5"))

# --------- İzmir Konak merkez ---------
KONAK_LAT = float(os.getenv("KONAK_LAT", "38.4192"))
KONAK_LON = float(os.getenv("KONAK_LON", "27.1287"))
KONAK_RADIUS_KM = float(os.getenv("KONAK_RADIUS_KM", "100"))
KONAK_LIST_N = int(os.getenv("KONAK_LIST_N", "5"))

# -------------------- YARDIMCILAR --------------------
def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))

def ensure_columns(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS earthquakes (
            event_time TEXT,
            latitude REAL,
            longitude REAL,
            magnitude REAL,
            depth_km REAL,
            location TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(earthquakes)")
    cols = {row[1] for row in cur.fetchall()}

    # Eksik kolonları ekle (eski db uyumu)
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
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_eq
        ON earthquakes(event_time, latitude, longitude, magnitude, depth_km, location)
        """
    )
    conn.commit()

def trim_db(conn, max_rows=50000):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM earthquakes")
    n = cur.fetchone()[0]
    if n <= max_rows:
        return
    to_delete = n - max_rows
    cur.execute(
        """
        DELETE FROM earthquakes
        WHERE rowid IN (
            SELECT rowid FROM earthquakes
            ORDER BY event_time ASC
            LIMIT ?
        )
        """,
        (to_delete,),
    )
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

        rows.append(
            {
                "event_time": dt.isoformat(),
                "latitude": lat,
                "longitude": lon,
                "depth_km": depth,
                "magnitude": mag,
                "location": location,
            }
        )

    return rows

def upsert_rows(conn, rows):
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        cur.execute(
            """
            INSERT OR IGNORE INTO earthquakes(event_time, latitude, longitude, magnitude, depth_km, location)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (r["event_time"], r["latitude"], r["longitude"], r["magnitude"], r["depth_km"], r["location"]),
        )
        if cur.rowcount == 1:
            inserted += 1
    conn.commit()
    return inserted

def get_last_n_near(conn, clat, clon, radius_km, n=5):
    # En yeni 800 içinden radius filtreleyip ilk n taneyi alıyoruz (hız için)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event_time, latitude, longitude, depth_km, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT 800
        """
    )
    cand = cur.fetchall()
    picked = []
    for et, lat, lon, depth, mag, loc in cand:
        d = haversine_km(clat, clon, lat, lon)
        if d <= radius_km:
            picked.append((et, lat, lon, depth, mag, loc, d))
        if len(picked) >= n:
            break
    return picked

def get_turkiye_window(conn, n=500):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event_time, latitude, longitude, depth_km, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT ?
        """,
        (n,),
    )
    return cur.fetchall()

def compute_alarm(max_mag, orange_thr, red_thr):
    if max_mag >= red_thr:
        return "RED"
    if max_mag >= orange_thr:
        return "ORANGE"
    return "NORMAL"

def alarm_badge(status: str) -> str:
    if status == "RED":
        return "🟥 *RED*"
    if status == "ORANGE":
        return "🟧 *ORANGE*"
    return "🟩 *NORMAL*"

def format_event_lines(events):
    out = []
    for et, lat, lon, depth, mag, loc, dist in events:
        try:
            dt = datetime.fromisoformat(et.replace("Z", "+00:00"))
            et_out = dt.strftime("%d.%m %H:%M")
        except Exception:
            et_out = et[:16]
        out.append(f"• *{mag:.1f}* | {et_out} | {depth:.1f} km | {dist:.0f} km")
        out.append(f"  {loc}")
    return out

def build_message(
    tr_status, tr_max, tr_orange_cnt, tr_red_cnt,
    bandirma_status, bandirma_max, bandirma_events,
    bursa_status, bursa_max, bursa_events,
    konak_status, konak_max, konak_events,
    inserted
):
    now_tr = datetime.now().strftime("%d.%m.%Y %H:%M")

    lines = []
    lines.append("📍 *Deprem Alarm Bot*")
    lines.append(f"🕒 {now_tr}")
    lines.append(f"➕ Yeni eklenen kayıt: *{inserted}*")
    lines.append("")
    lines.append(f"🇹🇷 Türkiye Alarm: {alarm_badge(tr_status)} (max Mw={tr_max:.1f}) | ORANGE+:{tr_orange_cnt} | RED+:{tr_red_cnt}")
    lines.append(f"📍 Bandırma Alarm: {alarm_badge(bandirma_status)} (max Mw={bandirma_max:.1f})")
    lines.append(f"📍 Bursa Alarm: {alarm_badge(bursa_status)} (max Mw={bursa_max:.1f})")
    lines.append(f"📍 İzmir Konak Alarm: {alarm_badge(konak_status)} (max Mw={konak_max:.1f})")
    lines.append("")

    # Bandırma listesi
    if bandirma_events:
        lines.append(f"📌 Bandırma {int(BANDIRMA_RADIUS_KM)} km içi *son {len(bandirma_events)} deprem*:")
        lines.extend(format_event_lines(bandirma_events))
    else:
        lines.append(f"📌 Bandırma {int(BANDIRMA_RADIUS_KM)} km içinde kayıt bulunamadı.")

    lines.append("")

    # Bursa listesi
    if bursa_events:
        lines.append(f"📌 Bursa {int(BURSA_RADIUS_KM)} km içi *son {len(bursa_events)} deprem*:")
        lines.extend(format_event_lines(bursa_events))
    else:
        lines.append(f"📌 Bursa {int(BURSA_RADIUS_KM)} km içinde kayıt bulunamadı.")

    lines.append("")

    # Konak listesi
    if konak_events:
        lines.append(f"📌 İzmir Konak {int(KONAK_RADIUS_KM)} km içi *son {len(konak_events)} deprem*:")
        lines.extend(format_event_lines(konak_events))
    else:
        lines.append(f"📌 İzmir Konak {int(KONAK_RADIUS_KM)} km içinde kayıt bulunamadı.")

    return "\n".join(lines)

def telegram_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Mesaj atlanıyor.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=20,
        )
        if r.status_code != 200:
            print("Telegram gönderim hatası:", r.status_code, r.text[:300])
            return False
        return True
    except Exception as e:
        print("Telegram gönderim exception:", e)
        return False

# -------------------- MAIN --------------------
def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)

    rows = parse_koeri()
    inserted = upsert_rows(conn, rows)
    trim_db(conn, MAX_ROWS)

    # Türkiye alarmı (son TR_WINDOW_N kayıt)
    tr_rows = get_turkiye_window(conn, TR_WINDOW_N)
    tr_max = max((r[4] for r in tr_rows), default=0.0)
    tr_orange_cnt = sum(1 for r in tr_rows if r[4] >= TR_ORANGE_MW)
    tr_red_cnt = sum(1 for r in tr_rows if r[4] >= TR_RED_MW)
    tr_status = compute_alarm(tr_max, TR_ORANGE_MW, TR_RED_MW)

    # Bandırma
    bandirma_events = get_last_n_near(conn, BANDIRMA_LAT, BANDIRMA_LON, BANDIRMA_RADIUS_KM, BANDIRMA_LIST_N)
    bandirma_max = max((ev[4] for ev in bandirma_events), default=0.0)
    bandirma_status = compute_alarm(bandirma_max, ORANGE_MW, RED_MW)

    # Bursa
    bursa_events = get_last_n_near(conn, BURSA_LAT, BURSA_LON, BURSA_RADIUS_KM, BURSA_LIST_N)
    bursa_max = max((ev[4] for ev in bursa_events), default=0.0)
    bursa_status = compute_alarm(bursa_max, ORANGE_MW, RED_MW)

    # İzmir Konak
    konak_events = get_last_n_near(conn, KONAK_LAT, KONAK_LON, KONAK_RADIUS_KM, KONAK_LIST_N)
    konak_max = max((ev[4] for ev in konak_events), default=0.0)
    konak_status = compute_alarm(konak_max, ORANGE_MW, RED_MW)

    print(f"KOERI parse: {min(500, len(rows))} | Yeni eklenen: {inserted}")

    # Telegram gönderim kuralı:
    # - FORCE_TELEGRAM=1 ise her zaman
    # - yoksa sadece (inserted>0) veya herhangi bir alarm ORANGE/RED ise
    send = (
        FORCE_TELEGRAM
        or inserted > 0
        or (tr_status in ("ORANGE", "RED"))
        or (bandirma_status in ("ORANGE", "RED"))
        or (bursa_status in ("ORANGE", "RED"))
        or (konak_status in ("ORANGE", "RED"))
    )

    if send:
        msg = build_message(
            tr_status, tr_max, tr_orange_cnt, tr_red_cnt,
            bandirma_status, bandirma_max, bandirma_events,
            bursa_status, bursa_max, bursa_events,
            konak_status, konak_max, konak_events,
            inserted
        )
        telegram_send(msg)

    conn.close()

if __name__ == "__main__":
    main()
