import os
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

# ===============================
# ENV (GitHub Actions Secrets)
# ===============================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

# ===============================
# Telegram
# ===============================
def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    requests.post(url, data=payload, timeout=20)

# ===============================
# Database
# ===============================
def init_db():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            lat REAL,
            lon REAL,
            depth REAL,
            magnitude REAL,
            location TEXT,
            source TEXT DEFAULT 'KOERI',
            UNIQUE(event_time, lat, lon, magnitude)
        )
    """)
    con.commit()
    con.close()

def insert_event(row):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO earthquakes
            (event_time, lat, lon, depth, magnitude, location, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, row)
        con.commit()
        inserted = cur.rowcount
    except Exception as e:
        print("DB hata:", e)
        inserted = 0
    con.close()
    return inserted

# ===============================
# Helpers
# ===============================
def _to_float(x):
    try:
        return float(x)
    except:
        return None

def _parse_line(ln: str):
    parts = ln.split()
    if len(parts) < 6:
        return None

    # ilk alanlar sabit
    date = parts[0]
    time = parts[1]
    lat = _to_float(parts[2])
    lon = _to_float(parts[3])
    depth = _to_float(parts[4])

    if lat is None or lon is None or depth is None:
        return None

    # KOERI: magnitudeler satırda birden fazla olabilir, "--" olabilir
    # Bu yüzden tüm tokenlarda sayısal olanları topla, mantıklı olanı seç.
    nums = []
    for tok in parts[5:]:
        v = _to_float(tok)
        if v is not None:
            nums.append(v)

    # magnitude bulunamazsa satırı geç
    if not nums:
        return None

    # genelde magnitude 0-10 arası olur, son uygun değeri seçelim
    mag = None
    for v in reversed(nums):
        if 0.0 <= v <= 10.0:
            mag = v
            break
    if mag is None:
        return None

    # location: sayısal kolonlardan sonra gelen metin; pratik çözüm:
    # 8. indeks ve sonrası gibi sabit değil; o yüzden orijinal satırdan date/time/lat/lon/depth kısmını düşüp kalan yazıyı al
    # Basit: ilk 5 tokenı at, gerisini string olarak al
    tail = " ".join(parts[5:])

    # "tail" içinde sayılar/-- var; location'ı düzgün almak için:
    # sondaki "Ilksel"/"Revize" gibi etiketi bırakabilir, sorun değil.
    location = tail

    # event_time
    event_time = datetime.strptime(f"{date} {time}", "%Y.%m.%d %H:%M:%S").isoformat() + "Z"

    return (event_time, lat, lon, depth, mag, location, "KOERI")

# ===============================
# Fetch KOERI
# ===============================
def fetch_koeri():
    headers = {"User-Agent": "Mozilla/5.0"}
    html = requests.get(KOERI_URL, headers=headers, timeout=30).content
    soup = BeautifulSoup(html, "html.parser")

    pre = soup.find("pre")
    if not pre:
        return []

    lines = pre.get_text().split("\n")
    events = []

    for ln in lines:
        ln = ln.strip()
        if not ln or ln.startswith("Tarih"):
            continue

        ev = _parse_line(ln)
        if ev:
            events.append(ev)

    return events

# ===============================
# Alarm Kontrol (şimdilik basit)
# ===============================
def check_alarm(event):
    _, _, _, _, mag, _, _ = event
    return mag >= 4.5

# ===============================
# MAIN
# ===============================
def main():
    init_db()
    events = fetch_koeri()

    if not events:
        print("KOERI'den veri çekilemedi veya parse edilemedi.")
        telegram_send("⚠️ KOERI verisi çekilemedi / parse edilemedi.")
        return

    max_fetched = max(e[0] for e in events)
    print("Fetch edilen en yeni zaman:", max_fetched)

    new_events = []
    inserted_count = 0
    for ev in events:
        inserted = insert_event(ev)
        inserted_count += inserted
        if inserted:
            new_events.append(ev)

    print("Yeni eklenen kayıt sayısı:", inserted_count)

    # Debug: her run’da küçük özet mesaj
    telegram_send(f"✅ DB Sync çalıştı.\nFetch: {len(events)} satır\nYeni eklenen: {inserted_count}\nFetch max: {max_fetched}")

    if not new_events:
        print("Yeni deprem yok")
        return

    alarm_events = [e for e in new_events if check_alarm(e)]
    if alarm_events:
        msg = "🚨 <b>DEPREM ALARM</b>\n\n"
        for e in alarm_events:
            t, lat, lon, d, m, loc, _src = e
            msg += (
                f"📍 {loc}\n"
                f"🕒 {t}\n"
                f"🌍 {lat},{lon}\n"
                f"📏 Derinlik: {d} km\n"
                f"📊 Mw: <b>{m}</b>\n\n"
            )
        telegram_send(msg)
    else:
        print("Alarm yok")

if __name__ == "__main__":
    main()
