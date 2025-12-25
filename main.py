import os
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import hashlib

# ===============================
# ENV (GitHub Actions Secrets)
# ===============================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DB_FILE = "deprem.db"
# KOERI: Son 500 deprem listesi (pre içinde)
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst6.asp"

# ===============================
# Telegram
# ===============================
def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID).")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=payload, timeout=20)
    except Exception as e:
        print("Telegram gönderim hatası:", e)

# ===============================
# Helpers
# ===============================
def iso_from_koeri(date_str: str, time_str: str) -> str:
    # KOERI format: YYYY.MM.DD HH:MM:SS
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y.%m.%d %H:%M:%S")
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def make_event_id(event_time: str, lat: float, lon: float, mag: float) -> str:
    s = f"{event_time}|{lat:.4f}|{lon:.4f}|{mag:.1f}"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def pick_mag(md: str, ml: str, mw: str) -> float:
    # KOERI sütunları bazen "--"
    for v in (mw, ml, md):
        if v and v != "--":
            try:
                return float(v)
            except:
                pass
    return 0.0

# ===============================
# Database
# ===============================
def init_db():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes (
            event_id TEXT PRIMARY KEY,
            event_time TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            depth REAL,
            magnitude REAL,
            location TEXT,
            source TEXT,
            UNIQUE(event_time, latitude, longitude, magnitude)
        )
    """)
    con.commit()
    con.close()

def insert_event(ev):
    # ev: (event_id, event_time, lat, lon, depth, mag, location, source)
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO earthquakes
            (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ev)
        con.commit()
        inserted = cur.rowcount
    except Exception as e:
        print("DB insert hata:", e)
        inserted = 0
    con.close()
    return inserted

# ===============================
# Fetch KOERI
# ===============================
def fetch_koeri():
    r = requests.get(KOERI_URL, timeout=30)
    r.encoding = "utf-8"  # KOERI türkçe karakterler için
    soup = BeautifulSoup(r.text, "html.parser")

    pre = soup.find("pre")
    if not pre:
        print("KOERI sayfasında <pre> bulunamadı.")
        return []

    lines = pre.get_text("\n").splitlines()
    events = []

    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        # Başlık/ayırıcı satırları ele
        if ln.startswith("Tarih") or "SON DEPREMLER" in ln or ln.startswith("----") or ln.startswith(".."):
            continue

        parts = ln.split()
        # Beklenen minimum kolon: date time lat lon depth md ml mw yer ... çözüm
        if len(parts) < 10:
            continue

        try:
            date_str = parts[0]          # 2025.12.25
            time_str = parts[1]          # 08:38:32
            lat = float(parts[2])
            lon = float(parts[3])
            depth = float(parts[4])

            md = parts[5] if len(parts) > 5 else "--"
            ml = parts[6] if len(parts) > 6 else "--"
            mw = parts[7] if len(parts) > 7 else "--"
            mag = pick_mag(md, ml, mw)

            # Son token genelde "İlksel" vb çözüm niteliği. Biz location içine katmıyoruz.
            # Location = parts[8:-1]
            location = " ".join(parts[8:-1]).strip()
            if not location:
                location = " ".join(parts[8:]).strip()

            event_time = iso_from_koeri(date_str, time_str)  # "YYYY-MM-DDTHH:MM:SS"
            event_id = make_event_id(event_time, lat, lon, mag)

            events.append((event_id, event_time, lat, lon, depth, mag, location, "KOERI"))
        except:
            continue

    return events

# ===============================
# Alarm Kontrol (şimdilik basit)
# ===============================
def check_alarm(ev):
    # ev: (event_id, event_time, lat, lon, depth, mag, location, source)
    mag = ev[5]
    return mag >= 4.5  # sonra turuncu/kırmızıya bağlarız

# ===============================
# MAIN
# ===============================
def main():
    init_db()
    events = fetch_koeri()

    if not events:
        print("KOERI'den veri gelmedi.")
        return

    new_events = []
    for ev in events:
        if insert_event(ev):
            new_events.append(ev)

    print(f"KOERI parse: {len(events)} satır, DB'ye yeni eklenen: {len(new_events)}")

    # Alarm varsa Telegram
    alarm_events = [e for e in new_events if check_alarm(e)]
    if alarm_events:
        msg = "🚨 <b>DEPREM ALARM</b>\n\n"
        for e in alarm_events:
            _, t, lat, lon, d, m, loc, src = e
            msg += (
                f"📍 {loc}\n"
                f"🕒 {t}\n"
                f"🌍 {lat},{lon}\n"
                f"📏 Derinlik: {d} km\n"
                f"📊 M: <b>{m}</b>\n"
                f"🔎 Kaynak: {src}\n\n"
            )
        telegram_send(msg)

if __name__ == "__main__":
    main()
