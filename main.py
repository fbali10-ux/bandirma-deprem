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

# Senin tarayıcıda gördüğün (25 Aralık kayıtları olan) sayfa
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst6.asp"

SOURCE_NAME = "KOERI"

# ===============================
# Telegram
# ===============================
def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID).")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, data=payload, timeout=20)
    except Exception as e:
        print("Telegram gönderim hatası:", e)

# ===============================
# Database
# ===============================
def init_db():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()

    # Senin DB şeman: event_id, event_time, latitude, longitude, depth, magnitude, location, source
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
            UNIQUE(event_time, latitude, longitude, magnitude, source)
        )
    """)

    con.commit()
    con.close()

def insert_event(ev):
    """
    ev: (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
    """
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO earthquakes
            (event_id, event_time, latitude, longitude, depth, magnitude, location, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ev)
        con.commit()
        inserted = cur.rowcount  # 1 ise eklendi
    except Exception as e:
        print("DB hata:", e)
        inserted = 0
    con.close()
    return inserted

# ===============================
# Fetch KOERI
# ===============================
def _to_float(x):
    try:
        return float(x)
    except:
        return None

def fetch_koeri():
    """
    KOERI <pre> tablosundan kayıtları okur.
    Beklenen kolonlar genel olarak:
    Tarih Saat Enlem(N) Boylam(E) Derinlik(km) MD ML Mw Yer ... Çözüm Niteliği(İlksel/Revize)
    Bazı satırlarda MD/ML/Mw '--' olabilir.
    """
    try:
        html = requests.get(KOERI_URL, timeout=30).content
    except Exception as e:
        print("KOERI erişim hatası:", e)
        return []

    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        print("KOERI sayfasında <pre> bulunamadı.")
        return []

    lines = pre.get_text().splitlines()
    events = []

    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue

        # başlık/ayraç satırları
        if ln.startswith("Tarih") or ln.startswith("-----") or "SON DEPREMLER" in ln or "Enlem" in ln:
            continue

        parts = ln.split()
        # minimum beklenen
        if len(parts) < 9:
            continue

        date = parts[0]     # 2025.12.25
        time = parts[1]     # 08:38:32

        lat = _to_float(parts[2])
        lon = _to_float(parts[3])
        if lat is None or lon is None:
            continue

        depth = _to_float(parts[4])
        if depth is None:
            depth = 0.0

        # MD/ML/Mw alanları çoğu zaman 5/6/7 index
        md = _to_float(parts[5]) if len(parts) > 5 else None
        ml = _to_float(parts[6]) if len(parts) > 6 else None
        mw = _to_float(parts[7]) if len(parts) > 7 else None

        mag = mw if mw is not None else (ml if ml is not None else md)
        if mag is None:
            continue

        # Location: parts[8].. son-1 (son token genelde İlksel/Revize)
        if len(parts) >= 10:
            location_tokens = parts[8:-1]
        else:
            location_tokens = parts[8:]
        location = " ".join(location_tokens).strip()

        # ISO zaman
        try:
            event_time = datetime.strptime(f"{date} {time}", "%Y.%m.%d %H:%M:%S").isoformat()
        except:
            continue

        # event_id üret (unique anahtar gibi)
        event_id = f"{event_time}_{lat:.4f}_{lon:.4f}_{mag:.1f}_{SOURCE_NAME}"
        event_id = event_id.replace(":", "").replace(".", "").replace("-", "").replace("T", "")

        events.append((event_id, event_time, lat, lon, depth, mag, location, SOURCE_NAME))

    return events

# ===============================
# Alarm Kontrol (şimdilik basit)
# ===============================
def check_alarm(ev):
    # ev: (event_id, event_time, lat, lon, depth, mag, location, source)
    mag = ev[5]
    return mag >= 4.5  # sonra turuncu/kırmızı kriterleri koyacağız

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

    print(f"Toplam çekilen: {len(events)} | Yeni eklenen: {len(new_events)}")

    if not new_events:
        print("Yeni deprem yok.")
        return

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
                f"📊 Mw/ML/MD: <b>{m}</b>\n"
                f"🛰 Kaynak: {src}\n\n"
            )
        telegram_send(msg)
    else:
        print("Alarm yok (yeni kayıt var ama eşik altı).")

if __name__ == "__main__":
    main()
