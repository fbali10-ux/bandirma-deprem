import os
import re
import math
import sqlite3
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

# ===================== AYARLAR =====================
KOERI_URL = os.getenv("KOERI_URL", "http://www.koeri.boun.edu.tr/scripts/lst6.asp")

DB_PATH = os.getenv("DB_PATH", "deprem.db")
MAX_ROWS = int(os.getenv("MAX_ROWS", "50000"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FORCE_TELEGRAM = os.getenv("FORCE_TELEGRAM", "0") == "1"

# Alarm eşikleri (merkezler için)
ORANGE_MW = float(os.getenv("ORANGE_MW", "5.0"))
RED_MW = float(os.getenv("RED_MW", "6.0"))

# Türkiye geneli eşikler
TR_ORANGE_MW = float(os.getenv("TR_ORANGE_MW", str(ORANGE_MW)))
TR_RED_MW = float(os.getenv("TR_RED_MW", str(RED_MW)))
TR_WINDOW_N = int(os.getenv("TR_WINDOW_N", "500"))

# -------- Merkezler (SON 2 DEPREM) --------
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9767"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "100"))
BANDIRMA_LIST_N = int(os.getenv("BANDIRMA_LIST_N", "2"))

BURSA_LAT = float(os.getenv("BURSA_LAT", "40.1950"))
BURSA_LON = float(os.getenv("BURSA_LON", "29.0600"))
BURSA_RADIUS_KM = float(os.getenv("BURSA_RADIUS_KM", "100"))
BURSA_LIST_N = int(os.getenv("BURSA_LIST_N", "2"))

KONAK_LAT = float(os.getenv("KONAK_LAT", "38.4192"))
KONAK_LON = float(os.getenv("KONAK_LON", "27.1287"))
KONAK_RADIUS_KM = float(os.getenv("KONAK_RADIUS_KM", "100"))
KONAK_LIST_N = int(os.getenv("KONAK_LIST_N", "2"))

# ===================== YARDIMCILAR =====================
def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))

def _table_exists(cur, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def _colnames(cur, table: str):
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def _index_exists(cur, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (name,))
    return cur.fetchone() is not None

def _dedupe_before_unique(cur):
    # Klasik yöntem: en küçük rowid kalsın, diğer mükerrerleri sil.
    # depth_km NULL olabilir; COALESCE ile 0.0 yapıp grupluyoruz.
    cur.execute("""
        DELETE FROM earthquakes
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM earthquakes
            GROUP BY
                event_time,
                latitude,
                longitude,
                magnitude,
                COALESCE(depth_km, 0.0),
                location
        )
    """)

def ensure_db(conn):
    """
    Amaç:
    1) Tabloyu garanti et
    2) Eski DB'lerde eksik kolon varsa migrate et (depth_km)
    3) UNIQUE INDEX oluşturmadan önce mükerrerleri temizle (Actions hatasını çözer)
    4) UNIQUE INDEX'i güvenle oluştur
    """
    cur = conn.cursor()

    # 1) Tablo yoksa oluştur
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
    conn.commit()

    # 2) Eski DB şeması: depth_km yoksa ekle
    cols = _colnames(cur, "earthquakes")
    if "depth_km" not in cols:
        # Eski tablolar (depth yerine vs.) için en güvenli: yeni kolonu ekle, NULL bırak
        cur.execute("ALTER TABLE earthquakes ADD COLUMN depth_km REAL")
        conn.commit()

    # 3) UNIQUE INDEX yoksa, önce mükerrer temizle, sonra index oluştur
    if not _index_exists(cur, "uq_eq"):
        # Önce temizlik
        _dedupe_before_unique(cur)
        conn.commit()

        # Sonra index
        # Eğer hâlâ mükerrer varsa (çok nadir), tekrar temizleyip tekrar deneyeceğiz.
        try:
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_eq
                ON earthquakes(event_time, latitude, longitude, magnitude, depth_km, location)
            """)
            conn.commit()
        except sqlite3.IntegrityError:
            # Son bir kez daha temizle ve tekrar dene
            _dedupe_before_unique(cur)
            conn.commit()
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_eq
                ON earthquakes(event_time, latitude, longitude, magnitude, depth_km, location)
            """)
            conn.commit()

def trim_db(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM earthquakes")
    n = cur.fetchone()[0]
    if n <= MAX_ROWS:
        return
    cur.execute("""
        DELETE FROM earthquakes
        WHERE rowid IN (
            SELECT rowid FROM earthquakes
            ORDER BY event_time ASC
            LIMIT ?
        )
    """, (n - MAX_ROWS,))
    conn.commit()

def parse_koeri():
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı (format değişmiş olabilir).")

    lines = [l.strip() for l in pre.get_text("\n").splitlines() if l.strip()]

    rows = []
    for ln in lines:
        if ln.startswith("Tarih") or ln.startswith("----"):
            continue

        p = re.split(r"\s+", ln)
        if len(p) < 8:
            continue

        # Örnek beklenen: YYYY.MM.DD HH:MM:SS LAT LON DEPTH MAG ... LOCATION...
        try:
            dt = datetime.strptime(p[0] + " " + p[1], "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
            lat = float(p[2])
            lon = float(p[3])
        except Exception:
            continue

        # depth bazen kayabilir; güvenli parse
        depth = None
        try:
            depth = float(p[4])
        except Exception:
            depth = None

        # Magnitude: 5-8 arası ilk float bul
        mag = None
        idx = None
        for i in range(5, min(10, len(p))):
            try:
                mag = float(p[i])
                idx = i
                break
            except Exception:
                pass
        if mag is None:
            continue

        loc = " ".join(p[idx + 1:]).strip() if idx is not None else "-"
        if not loc:
            loc = "-"

        rows.append((dt.isoformat(), lat, lon, mag, depth, loc))
        if len(rows) >= 500:
            break

    return rows

def upsert(conn, rows):
    cur = conn.cursor()
    added = 0
    for r in rows:
        # depth None olabilir; DB'ye NULL gitsin
        cur.execute("INSERT OR IGNORE INTO earthquakes VALUES (?, ?, ?, ?, ?, ?)", r)
        if cur.rowcount == 1:
            added += 1
    conn.commit()
    return added

def last_n_near(conn, lat0, lon0, radius, n):
    cur = conn.cursor()
    cur.execute("""
        SELECT event_time, latitude, longitude, depth_km, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT 800
    """)
    out = []
    for et, lat, lon, depth, mag, loc in cur.fetchall():
        dist = haversine_km(lat0, lon0, lat, lon)
        if dist <= radius:
            out.append((et, depth, mag, loc, dist))
        if len(out) >= n:
            break
    return out

def compute_alarm_label(max_mag, orange_thr, red_thr):
    # İstenen net format:
    # 🟩 NORMAL / 🟧 ORANGE / 🟥 RED
    if max_mag >= red_thr:
        return "🟥 *RED*"
    if max_mag >= orange_thr:
        return "🟧 *ORANGE*"
    return "🟩 *NORMAL*"

def fmt_events(events):
    lines = []
    for et, depth, mag, loc, dist in events:
        try:
            t = datetime.fromisoformat(et.replace("Z", "+00:00")).strftime("%d.%m %H:%M")
        except Exception:
            t = et[:16]

        d = 0.0 if depth is None else float(depth)
        lines.append(f"• *{mag:.1f}* | {t} | {d:.1f} km | {dist:.0f} km")
        # Telegram'da lokasyon yazısı renklendirilemez; ama kalın/italik yapılabilir
        lines.append(f"  _{loc}_")
    return lines

def send_telegram(text):
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

# ===================== MAIN =====================
def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_db(conn)

    rows = parse_koeri()
    added = upsert(conn, rows)
    trim_db(conn)

    # Türkiye alarmı (son TR_WINDOW_N kayıt)
    cur = conn.cursor()
    cur.execute("""
        SELECT magnitude FROM earthquakes
        ORDER BY event_time DESC
        LIMIT ?
    """, (TR_WINDOW_N,))
    mags = [r[0] for r in cur.fetchall()]
    tr_max = max(mags) if mags else 0.0
    tr_alarm = compute_alarm_label(tr_max, TR_ORANGE_MW, TR_RED_MW)

    # Merkezler
    bandirma = last_n_near(conn, BANDIRMA_LAT, BANDIRMA_LON, BANDIRMA_RADIUS_KM, BANDIRMA_LIST_N)
    bursa = last_n_near(conn, BURSA_LAT, BURSA_LON, BURSA_RADIUS_KM, BURSA_LIST_N)
    konak = last_n_near(conn, KONAK_LAT, KONAK_LON, KONAK_RADIUS_KM, KONAK_LIST_N)

    bandirma_max = max((e[2] for e in bandirma), default=0.0)
    bursa_max = max((e[2] for e in bursa), default=0.0)
    konak_max = max((e[2] for e in konak), default=0.0)

    bandirma_alarm = compute_alarm_label(bandirma_max, ORANGE_MW, RED_MW)
    bursa_alarm = compute_alarm_label(bursa_max, ORANGE_MW, RED_MW)
    konak_alarm = compute_alarm_label(konak_max, ORANGE_MW, RED_MW)

    print(f"KOERI parse: {min(500, len(rows))} | Yeni eklenen: {added}")

    msg = []
    msg.append("📍 *Deprem Alarm Bot*")
    msg.append(datetime.now().strftime("🕒 %d.%m.%Y %H:%M"))
    msg.append(f"🇹🇷 Türkiye Alarm: {tr_alarm} (max Mw={tr_max:.1f})")
    msg.append("")

    msg.append(f"🟦 *Bandırma* Alarm: {bandirma_alarm}")
    msg.extend(fmt_events(bandirma) if bandirma else ["• Kayıt yok"])
    msg.append("")

    msg.append(f"🟨 *Bursa* Alarm: {bursa_alarm}")
    msg.extend(fmt_events(bursa) if bursa else ["• Kayıt yok"])
    msg.append("")

    msg.append(f"🟪 *İzmir Konak* Alarm: {konak_alarm}")
    msg.extend(fmt_events(konak) if konak else ["• Kayıt yok"])

    # Telegram gönderim kuralı:
    # - FORCE_TELEGRAM=1 ise her zaman
    # - yoksa sadece (added>0) veya herhangi bir alarm ORANGE/RED ise
    send = (
        FORCE_TELEGRAM
        or added > 0
        or ("*ORANGE*" in tr_alarm) or ("*RED*" in tr_alarm)
        or ("*ORANGE*" in bandirma_alarm) or ("*RED*" in bandirma_alarm)
        or ("*ORANGE*" in bursa_alarm) or ("*RED*" in bursa_alarm)
        or ("*ORANGE*" in konak_alarm) or ("*RED*" in konak_alarm)
    )

    if send:
        send_telegram("\n".join(msg))

    conn.close()

if __name__ == "__main__":
    main()
