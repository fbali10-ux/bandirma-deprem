import os
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup

from turkiye_alarm import turkey_alarm  # alarm/rapor üretici


# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Alarm yoksa bile "durum mesajı" (alarm özeti) atılsın mı?
# 0 = sadece SON 5 DEPREM gönder
# 1 = alarm özeti + SON 5 DEPREM gönder
HOURLY_STATUS = int(os.getenv("HOURLY_STATUS", "0"))

BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9700"))

DB_FILE = os.getenv("DB_FILE", "deprem.db")
KOERI_URL = os.getenv("KOERI_URL", "http://www.koeri.boun.edu.tr/scripts/lst9.asp")


# =========================
# Telegram
# =========================
def telegram_send(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID).")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=30)
    if r.status_code != 200:
        print("Telegram hata:", r.status_code, r.text[:500])
        return False
    return True


# =========================
# DB
# =========================
def init_db(db_file: str):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute("""
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
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_eq_time ON earthquakes(event_time)")
    con.commit()
    con.close()


def upsert_rows(db_file: str, rows: list[dict]) -> int:
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    inserted = 0
    for r in rows:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO earthquakes
                (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r["event_id"],
                r["event_time"],
                r["latitude"],
                r["longitude"],
                r["depth"],
                r["magnitude"],
                r["location"],
                r.get("source", "KOERI"),
            ))
            if cur.rowcount == 1:
                inserted += 1
        except Exception as e:
            print("DB insert hata:", e)

    con.commit()
    con.close()
    return inserted


# =========================
# KOERI parse
# =========================
def parse_koeri() -> list[dict]:
    html = requests.get(KOERI_URL, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı. Format değişmiş olabilir.")

    lines = [ln.rstrip("\n") for ln in pre.get_text("\n").splitlines()]
    data = []

    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue

        # KOERI satırları genelde şöyle:
        # 2025.12.25 14:31:12  40.1234  27.1234  7.8  -.-  2.6  -.-  YER (IL) ...
        parts = ln.split()
        if len(parts) < 8:
            continue

        # tarih + saat
        if "." not in parts[0] or ":" not in parts[1]:
            continue

        date_str = parts[0].replace(".", "-")
        time_str = parts[1]
        dt_iso = f"{date_str}T{time_str}"

        # lat lon depth
        try:
            lat = float(parts[2])
            lon = float(parts[3])
            depth = float(parts[4])
        except:
            continue

        # Magnitude: KOERI bazı satırlarda 6. sütun/7. sütun gibi oynayabiliyor
        # Güvenli yaklaşım: ilk bulunabilen "float" magnitude'u al
        mag = None
        for p in parts[5:9]:
            try:
                v = float(p)
                # magnitude mantıklı aralık
                if 0.0 <= v <= 10.0:
                    mag = v
                    break
            except:
                pass
        if mag is None:
            continue

        # Location: satırın geri kalanı
        # en basit: ilk 9 parçadan sonrası
        location = " ".join(parts[9:]) if len(parts) > 9 else " ".join(parts[8:])

        # event_id: aynı saniyede aynı koordinat/mag için stabil anahtar
        event_id = f"{dt_iso}_{lat:.4f}_{lon:.4f}_{mag:.1f}_{depth:.1f}"

        data.append({
            "event_id": event_id,
            "event_time": dt_iso,
            "latitude": lat,
            "longitude": lon,
            "depth": depth,
            "magnitude": mag,
            "location": location.strip()[:250],
            "source": "KOERI"
        })

    return data


# =========================
# SON 5 DEPREM
# =========================
def build_last5_message(db_file: str) -> str:
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute("""
        SELECT event_time, latitude, longitude, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT 5
    """)
    rows = cur.fetchall()
    con.close()

    if not rows:
        return "📭 *SON 5 DEPREM*\nVeri yok."

    msg = ["📌 *SON 5 DEPREM*"]
    for i, (t, lat, lon, mag, loc) in enumerate(rows, 1):
        # event_time ISO: 2025-12-25T14:31:12
        try:
            dt = datetime.fromisoformat(str(t).replace("Z", ""))
            t_str = dt.strftime("%d.%m.%Y %H:%M:%S")
        except:
            t_str = str(t)

        try:
            mag_str = f"{float(mag):.1f}"
        except:
            mag_str = str(mag)

        msg.append(f"{i}) {t_str} | *M{mag_str}*\n   📍 {loc} ({float(lat):.2f}, {float(lon):.2f})")

    return "\n".join(msg)


# =========================
# MAIN
# =========================
def main():
    init_db(DB_FILE)

    # KOERI çek + DB yaz
    rows = parse_koeri()
    ins = upsert_rows(DB_FILE, rows)
    print(f"KOERI satır: {len(rows)} | Yeni eklenen: {ins}")

    # Alarm metni üret (Bandırma + Türkiye geneli özet senin turkiye_alarm.py içinde)
    # turkey_alarm fonksiyonunun döndürdüğü metni olduğu gibi gönderiyoruz.
    alarm_text = turkey_alarm(
        db_file=DB_FILE,
        bandirma_lat=BANDIRMA_LAT,
        bandirma_lon=BANDIRMA_LON,
        bandirma_radius_km=70.0
    )

    last5_text = build_last5_message(DB_FILE)

    # Telegram gönderme kuralı:
    # - Alarm varsa: alarm + last5
    # - Alarm yoksa:
    #   HOURLY_STATUS=1 ise "alarm yok" + last5
    #   HOURLY_STATUS=0 ise sadece last5
    if alarm_text and alarm_text.strip():
        final_msg = alarm_text.strip() + "\n\n" + last5_text
        telegram_send(final_msg)
    else:
        if HOURLY_STATUS == 1:
            telegram_send("✅ Alarm yok.\n\n" + last5_text)
        else:
            telegram_send(last5_text)


if __name__ == "__main__":
    main()
