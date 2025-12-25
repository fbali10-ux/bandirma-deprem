import os
import sqlite3
import requests
from bs4 import BeautifulSoup

# turkiye_alarm modülünü böyle import edeceğiz:
# içindeki fonksiyonun adı turkiye_alarm mı turkey_alarm mı fark etmeyecek
import turkiye_alarm as alarm_mod

# =========================
# ENV / SABİTLER
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9700"))

DB_FILE = os.getenv("DB_FILE", "deprem.db")
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"


# =========================
# TELEGRAM
# =========================
def telegram_send(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        r = requests.post(url, data=payload, timeout=30)
        if r.status_code != 200:
            print("Telegram hata:", r.text)
            return False
        return True
    except Exception as e:
        print("Telegram exception:", e)
        return False


# =========================
# DB
# =========================
def init_db(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS earthquakes (
            event_id   TEXT PRIMARY KEY,
            event_time TEXT,
            latitude   REAL,
            longitude  REAL,
            depth      REAL,
            magnitude  REAL,
            location   TEXT,
            source     TEXT
        )
        """
    )
    con.commit()
    con.close()


def upsert_rows(db_path: str, rows: list[dict]) -> int:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    inserted = 0

    for r in rows:
        cur.execute(
            """
            INSERT OR IGNORE INTO earthquakes
            (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["event_id"],
                r["event_time"],
                r["latitude"],
                r["longitude"],
                r["depth"],
                r["magnitude"],
                r["location"],
                r.get("source", "KOERI"),
            ),
        )
        if cur.rowcount == 1:
            inserted += 1

    con.commit()
    con.close()
    return inserted


def fetch_last_n(db_path: str, limit: int = 5) -> list[dict]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        """
        SELECT event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


# =========================
# KOERI PARSE
# =========================
def parse_koeri() -> list[dict]:
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")

    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı. Sayfa formatı değişmiş olabilir.")

    lines = [ln.strip() for ln in pre.get_text("\n").splitlines() if ln.strip()]

    out: list[dict] = []
    for ln in lines:
        # Satır tarih ile başlamıyorsa geç
        if len(ln) < 20:
            continue
        if ln[2] != "." or ln[5] != ".":
            continue

        parts = ln.split()
        if len(parts) < 7:
            continue

        dt_str = f"{parts[0]} {parts[1]}"
        lat_s, lon_s, depth_s, mag_s = parts[2], parts[3], parts[4], parts[5]
        loc = " ".join(parts[6:]).strip()

        try:
            latitude = float(lat_s.replace(",", "."))
            longitude = float(lon_s.replace(",", "."))
            depth = float(depth_s.replace(",", "."))
            magnitude = float(mag_s.replace(",", "."))
        except:
            continue

        # deterministik event_id
        event_id = f"{dt_str}|{latitude:.4f}|{longitude:.4f}|{magnitude:.1f}"

        out.append(
            {
                "event_id": event_id,
                "event_time": dt_str,
                "latitude": latitude,
                "longitude": longitude,
                "depth": depth,
                "magnitude": magnitude,
                "location": loc,
                "source": "KOERI",
            }
        )

    return out


# =========================
# ALARM FN SEÇ (turkiye_alarm mı turkey_alarm mı)
# =========================
def get_alarm_function():
    fn = getattr(alarm_mod, "turkiye_alarm", None)
    if fn is None:
        fn = getattr(alarm_mod, "turkey_alarm", None)
    return fn


# =========================
# MAIN
# =========================
def main():
    init_db(DB_FILE)

    rows = parse_koeri()
    inserted = upsert_rows(DB_FILE, rows)
    print(f"KOERI satır: {len(rows)} | Yeni eklenen: {inserted}")

    # 1) Her çalışmada SON 5 depremi Telegram'a bas (güncel mi diye kontrol)
    last5 = fetch_last_n(DB_FILE, 5)
    if last5:
        lines = []
        for r in last5:
            try:
                mag = float(r.get("magnitude", 0))
                dep = float(r.get("depth", 0))
                lat = float(r.get("latitude", 0))
                lon = float(r.get("longitude", 0))
            except:
                mag, dep, lat, lon = 0, 0, 0, 0

            lines.append(
                f'{r.get("event_time")} | M{mag:.1f} | {dep:.1f}km | {lat:.3f},{lon:.3f} | {r.get("location","")}'
            )

        telegram_send("📌 Son 5 deprem (KOERI DB):\n" + "\n".join(lines))

    # 2) Alarm raporu çalıştır (varsa mesajı gönder)
    alarm_fn = get_alarm_function()
    if alarm_fn is None:
        print("turkiye_alarm.py içinde turkiye_alarm / turkey_alarm fonksiyonu bulunamadı.")
        return

    try:
        # En güvenlisi: pozisyonel çağrı (keyword hatası yaşamayalım)
        # Beklenen: (db_file, center_lat, center_lon, radius_km)
        has_alarm, msg = alarm_fn(DB_FILE, BANDIRMA_LAT, BANDIRMA_LON, 70.0)
        if msg:
            telegram_send(msg)
        else:
            print("Alarm yok / mesaj boş.")
    except TypeError:
        # Eğer senin fonksiyon imzası farklıysa, ikinci bir deneme yapalım:
        has_alarm, msg = alarm_fn(DB_FILE, BANDIRMA_LAT, BANDIRMA_LON)
        if msg:
            telegram_send(msg)
    except Exception as e:
        print("Alarm hesaplama hatası:", e)


if __name__ == "__main__":
    main()
