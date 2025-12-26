# main.py
import os
import sqlite3
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from turkiye_alarm import turkiye_alarm  # <-- DOĞRU import

# -----------------------------
# ENV
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN".upper())
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")   or os.getenv("TELEGRAM_CHAT_ID".upper())

BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9700"))

# 1 ise her çalışmada “son 5 deprem” mesajını da gönderir.
HOURLY_STATUS = str(os.getenv("HOURLY_STATUS", "0")).strip() == "1"

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"


# -----------------------------
# Telegram
# -----------------------------
def telegram_send(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": CHAT_ID, "text": text}, timeout=30)
    if r.status_code != 200:
        print("Telegram hata:", r.status_code, r.text[:2000])
        return False
    return True


# -----------------------------
# DB
# -----------------------------
def init_db(db_file: str):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS earthquakes (
            event_id TEXT PRIMARY KEY,
            event_time TEXT,
            latitude REAL,
            longitude REAL,
            depth REAL,
            magnitude REAL,
            location TEXT,
            source TEXT
        )
        """
    )
    con.commit()
    con.close()

def upsert_quakes(db_file: str, rows):
    """
    rows: list of dict with keys:
      event_id, event_time(ISO+00:00), latitude, longitude, depth, magnitude, location, source
    """
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    ins = 0
    for r in rows:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO earthquakes
                (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["event_id"],
                    r["event_time"],
                    float(r["latitude"]),
                    float(r["longitude"]),
                    float(r["depth"]),
                    float(r["magnitude"]),
                    r.get("location", ""),
                    r.get("source", "KOERI"),
                ),
            )
            if cur.rowcount == 1:
                ins += 1
        except Exception as e:
            print("DB insert hata:", e)

    con.commit()
    con.close()
    return ins


# -----------------------------
# KOERI Parse
# -----------------------------
def _to_iso_utc(dt_str: str) -> str:
    """
    KOERI tarih formatı değişebiliyor.
    En yaygın: 'YYYY.MM.DD HH:MM:SS'
    """
    s = dt_str.strip()
    # KOERI bazen arada çok boşluk bırakıyor
    s = " ".join(s.split())
    # örn 2025.12.26 12:34:56
    dt = datetime.strptime(s, "%Y.%m.%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="seconds")

def fetch_koeri_last500():
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise SystemExit("KOERI sayfasında <pre> bulunamadı (format değişmiş olabilir).")

    lines = [ln.rstrip("\n") for ln in pre.get_text("\n").splitlines()]
    # İlk satırlar başlık olur; veri satırlarını yakalamaya çalış
    data = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        # başlık satırlarını ele
        if ln.lower().startswith("tarih") or ln.startswith("-"):
            continue
        parts = ln.split()
        # beklenen minimum kolon
        if len(parts) < 8:
            continue

        # KOERI tipik: Tarih Saat Enlem Boylam Derinlik ... Mw Yer
        # Tarih + Saat:
        dt = f"{parts[0]} {parts[1]}"
        try:
            event_time = _to_iso_utc(dt)
        except:
            continue

        try:
            lat = float(parts[2])
            lon = float(parts[3])
            depth = float(parts[4])
        except:
            continue

        # Mw/Md gibi büyüklük kolonunu yakalamaya çalış
        mag = None
        for p in parts[5:10]:
            try:
                mag = float(p.replace(",", "."))
                break
            except:
                pass
        if mag is None:
            continue

        # location genelde satırın son tarafı
        loc = " ".join(parts[10:]) if len(parts) > 10 else ""

        event_id = f"KOERI_{event_time}_{lat}_{lon}_{depth}_{mag}".replace(":", "").replace("+", "")
        data.append(
            {
                "event_id": event_id,
                "event_time": event_time,
                "latitude": lat,
                "longitude": lon,
                "depth": depth,
                "magnitude": mag,
                "location": loc,
                "source": "KOERI",
            }
        )

    # KOERI zaten “son X” verir; biz makul bir sayı keselim
    return data[:500]


# -----------------------------
# Main
# -----------------------------
def main():
    init_db(DB_FILE)

    koeri_rows = fetch_koeri_last500()
    added = upsert_quakes(DB_FILE, koeri_rows)
    print(f"KOERI satır: {len(koeri_rows)} | Yeni eklenen: {added}")

    has_alarm, alarm_msg, last5_block = turkiye_alarm(DB_FILE, BANDIRMA_LAT, BANDIRMA_LON, 70.0)

    # 1) Alarm varsa HER ZAMAN gönder
    if has_alarm:
        telegram_send(alarm_msg)
        return

    # 2) Alarm yoksa HOURLY_STATUS=1 ise son 5 depremi gönder
    if HOURLY_STATUS:
        telegram_send(last5_block)
        print("Alarm yok ama son 5 deprem gönderildi (HOURLY_STATUS=1).")
    else:
        print("Alarm yok (HOURLY_STATUS=0 olduğu için mesaj gönderilmedi).")


if __name__ == "__main__":
    main()
