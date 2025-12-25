import os
import sqlite3
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from turkiye_alarm import evaluate_turkiye_clusters, evaluate_bandirma_alarm

# ===============================
# ENV (GitHub Actions Secrets)
# ===============================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

# Bandırma merkez (ENV ile override edilebilir)
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3529"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9767"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "70"))

# Türkiye küme yarıçapı
TR_CLUSTER_RADIUS_KM = float(os.getenv("TR_CLUSTER_RADIUS_KM", "70"))

# ===============================
# Telegram
# ===============================
def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, data=payload, timeout=20)
    print("Telegram:", r.status_code)

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

def get_rows_last_days(days: int):
    since = (datetime.utcnow() - timedelta(days=days)).isoformat(timespec="seconds")
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("""
        SELECT event_time, lat, lon, depth, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
    """, (since,))
    rows = cur.fetchall()
    con.close()
    return rows

# ===============================
# Fetch KOERI
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
        if not ln or ln.startswith("Tarih") or ln.startswith("Date"):
            continue

        parts = ln.split()
        if len(parts) < 8:
            continue

        try:
            date = parts[0]  # 2025.12.25
            time = parts[1]  # 08:38:32
            lat = float(parts[2])
            lon = float(parts[3])
            depth = float(parts[4])

            # büyüklük: güvenli tarama
            mag = None
            for tok in parts[5:12]:
                try:
                    v = float(tok)
                    if 0.0 <= v <= 10.0:
                        mag = v
                        break
                except:
                    pass
            if mag is None:
                mag = float(parts[6])

            location = " ".join(parts[7:]).strip()

            event_time = datetime.strptime(f"{date} {time}", "%Y.%m.%d %H:%M:%S") \
                                 .isoformat(timespec="seconds")

            events.append((event_time, lat, lon, depth, mag, location, "KOERI"))
        except:
            continue

    return events

# ===============================
# Mesaj formatları
# ===============================
def _fmt_event(e):
    if not e:
        return "-"
    t = e["t"].strftime("%Y-%m-%d %H:%M:%S")
    m = e["mag"]
    loc = (e.get("location") or "").strip()
    return f"{t} | Mw {m:.1f} | {loc}"

def build_message(tr: dict, bd: dict) -> str:
    now = tr["now_utc"].strftime("%Y-%m-%d %H:%M:%S")

    # 1) Bandırma başlığı
    bd_state = "🔴 <b>KIRMIZI</b>" if bd["red"] else ("🟠 <b>TURUNCU</b>" if bd["orange"] else "✅ <b>NORMAL</b>")
    msg = (
        f"🕒 UTC: {now}\n\n"
        f"📍 <b>Bandırma 70 km Alarm</b> ({bd_state})\n"
        f"Merkez: {BANDIRMA_latitude:.4f},{BANDIRMA_longitude:.4f} | Yarıçap: {bd['radius_km']} km\n"
    )

    c = bd["counts"]
    msg += (
        f"• 24s: M≥3={c['24h_M>=3']} | M≥4={c['24h_M>=4']} | maxMw={c['24h_max'] if c['24h_max'] is not None else '-'}\n"
        f"• 7g : M≥3={c['7d_M>=3']} | M≥4={c['7d_M>=4']} | M≥6.5={c['7d_M>=6.5']}\n"
        f"• 30g: M≥5={c['30d_M>=5']} | M≥5.8={c['30d_M>=5.8']}\n"
    )

    if bd["red"]:
        msg += "\n🔴 <b>Kırmızı neden(ler):</b>\n" + "\n".join([f"• {r}" for r in bd["red_reasons"]]) + "\n"
    elif bd["orange"]:
        msg += "\n🟠 <b>Turuncu neden(ler):</b>\n" + "\n".join([f"• {r}" for r in bd["orange_reasons"]]) + "\n"
    else:
        msg += "\n✅ Alarm yok.\n"

    msg += (
        f"\nEn büyük 24s: {_fmt_event(bd['top']['24h'])}\n"
        f"En büyük 7g : {_fmt_event(bd['top']['7d'])}\n"
        f"En büyük 30g: {_fmt_event(bd['top']['30d'])}\n"
    )

    # 2) Türkiye geneli başlığı
    reds = tr["red"]
    oranges = tr["orange"]
    msg += (
        f"\n\n🇹🇷 <b>Türkiye Geneli Küme Alarmı</b> (Küme yarıçapı: {tr['radius_km']} km)\n"
        f"🔴 Kırmızı küme: <b>{len(reds)}</b> | 🟠 Turuncu küme: <b>{len(oranges)}</b>\n"
        f"(Turuncu: 30g maxMw≥5.0 | Kırmızı: 14g maxMw≥5.5)\n"
    )

    # kırmızı top 3
    if reds:
        msg += "\n🔴 <b>KIRMIZI (Top 3)</b>\n"
        for i, cl in enumerate(reds[:3], 1):
            top = cl["red_top"] or cl["orange_top"]
            msg += f"{i}) {_fmt_event(top)} | (kayıt: {cl['count_redwin']})\n"
    else:
        msg += "\n🔴 Kırmızı küme yok.\n"

    # turuncu top 3
    if oranges:
        msg += "\n🟠 <b>TURUNCU (Top 3)</b>\n"
        for i, cl in enumerate(oranges[:3], 1):
            top = cl["orange_top"]
            msg += f"{i}) {_fmt_event(top)} | (kayıt: {cl['count_30d']})\n"
    else:
        msg += "\n🟠 Turuncu küme yok.\n"

    return msg

# ===============================
# MAIN
# ===============================
def main():
    init_db()

    # 1) KOERI -> DB sync
    inserted_total = 0
    try:
        events = fetch_koeri()
        for ev in events:
            inserted_total += insert_event(ev)
        print("KOERI satır:", len(events), "Yeni eklenen:", inserted_total)
    except Exception as e:
        print("KOERI fetch hata:", e)

    # 2) Analizler DB üzerinden (bandırma 30 gün + tr 30 gün yeterli)
    rows_30d = get_rows_last_days(30)

    now = datetime.utcnow()

    # Bandırma (70 km) - senin kriterlerin
    bd = evaluate_bandirma_alarm(
        db_rows=rows_30d,
        center_lat=BANDIRMA_LAT,
        center_lon=BANDIRMA_LON,
        radius_km=BANDIRMA_RADIUS_KM,
        now=now
    )

    # Türkiye genel küme (70 km) - turuncu/kırmızı eşikleri
    tr = evaluate_turkiye_clusters(
        db_rows=rows_30d,
        radius_km=TR_CLUSTER_RADIUS_KM,
        orange_mag=5.0,
        orange_days=30,
        red_mag=5.5,
        red_days=14,
        now=now
    )

    # 3) Tek mesaj Telegram
    msg = build_message(tr, bd)
    telegram_send(msg)

if __name__ == "__main__":
    main()
