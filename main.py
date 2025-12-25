import os
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup

from turkiye_alarm import build_report

# ===============================
# ENV
# ===============================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9700"))

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

# Varsayılan: sadece alarm olunca gönder
# HOURLY_STATUS=1 yaparsan her çalışmada durum mesajı yollar
HOURLY_STATUS = os.getenv("HOURLY_STATUS", "0") == "1"


# ===============================
# Telegram
# ===============================
def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram ENV eksik")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=payload, timeout=25)
    except Exception as e:
        print("Telegram gönderim hatası:", e)


# ===============================
# DB: init + migrate (lat/lon -> latitude/longitude)
# ===============================
def _table_cols(con, table: str):
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def init_db_and_migrate():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()

    # tablo yoksa oluştur
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS earthquakes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            latitude REAL,
            longitude REAL,
            depth REAL,
            magnitude REAL,
            location TEXT,
            UNIQUE(event_time, latitude, longitude, magnitude)
        )
        """
    )
    con.commit()

    cols = _table_cols(con, "earthquakes")

    # Eğer eski şema varsa (lat/lon), otomatik migrate et
    if ("lat" in cols or "lon" in cols) and ("latitude" not in cols or "longitude" not in cols):
        print("DB migrate: lat/lon -> latitude/longitude")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS earthquakes_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time TEXT,
                latitude REAL,
                longitude REAL,
                depth REAL,
                magnitude REAL,
                location TEXT,
                UNIQUE(event_time, latitude, longitude, magnitude)
            )
            """
        )
        con.commit()

        # Eski kolon adlarını okuyup uygun şekilde kopyala
        # Eski: event_time, lat, lon, depth, magnitude, location
        cur.execute(
            """
            INSERT OR IGNORE INTO earthquakes_new (event_time, latitude, longitude, depth, magnitude, location)
            SELECT event_time, lat, lon, depth, magnitude, location
            FROM earthquakes
            """
        )
        con.commit()

        cur.execute("DROP TABLE earthquakes")
        cur.execute("ALTER TABLE earthquakes_new RENAME TO earthquakes")
        con.commit()

    con.close()


def insert_event(event):
    """
    event: (event_time, latitude, longitude, depth, magnitude, location)
    """
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    try:
        cur.execute(
            """
            INSERT OR IGNORE INTO earthquakes
            (event_time, latitude, longitude, depth, magnitude, location)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            event,
        )
        con.commit()
        inserted = cur.rowcount
    except Exception as e:
        print("DB hata:", e)
        inserted = 0
    con.close()
    return inserted


# ===============================
# KOERI fetch
# ===============================
def fetch_koeri():
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        return []

    lines = pre.get_text().splitlines()
    events = []

    for ln in lines:
        ln = ln.strip()
        if not ln or ln.startswith("Tarih") or ln.startswith("----"):
            continue

        parts = ln.split()
        # Beklenen baş: tarih saat enlem boylam derinlik (sonra md ml mw)
        if len(parts) < 8:
            continue

        try:
            date = parts[0]          # 2025.12.25
            time = parts[1]          # 08:38:32
            lat = float(parts[2])
            lon = float(parts[3])
            depth = float(parts[4])

            md_s = parts[5]
            ml_s = parts[6]
            mw_s = parts[7]

            def to_float(x):
                try:
                    if x in ["-.-", "--", "nan"]:
                        return None
                    return float(x)
                except Exception:
                    return None

            md = to_float(md_s)
            ml = to_float(ml_s)
            mw = to_float(mw_s)

            # magnitude seçimi: Mw > ML > MD
            mag = mw if mw is not None else (ml if ml is not None else (md if md is not None else None))
            if mag is None:
                continue

            # yer bilgisi: 8. elemandan itibaren
            location = " ".join(parts[8:]).strip()

            dt = datetime.strptime(f"{date} {time}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
            event_time = dt.replace(microsecond=0).isoformat()

            events.append((event_time, lat, lon, depth, float(mag), location))
        except Exception:
            continue

    return events


# ===============================
# MAIN
# ===============================
def main():
    init_db_and_migrate()

    events = fetch_koeri()
    new_count = 0
    for ev in events:
        new_count += insert_event(ev)

    print(f"KOERI satır: {len(events)} | Yeni eklenen: {new_count}")

    # Alarm raporu üret
    has_alarm, msg = build_report(DB_FILE, BANDIRMA_LAT, BANDIRMA_LON, radius_km=70.0)

    if has_alarm or HOURLY_STATUS:
        telegram_send(msg)
    else:
        print("Alarm yok (HOURLY_STATUS=0 olduğu için mesaj gönderilmedi)")


if __name__ == "__main__":
    main()
