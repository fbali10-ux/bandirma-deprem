import os
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from math import radians, sin, cos, sqrt, atan2

# ===============================
# ENV
# ===============================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Bandırma merkez noktası (Actions Variables veya Secrets)
# Settings -> Secrets and variables -> Actions -> Variables:
# BANDIRMA_LAT, BANDIRMA_LON
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.352"))   # yaklaşık Bandırma
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.976"))

DB_FILE = "deprem.db"

# KOERI (500 satır) - senin kullandığın
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst16.asp"

# ===============================
# Telegram
# ===============================
def telegram_send(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML", "disable_web_page_preview": True}
    r = requests.post(url, data=payload, timeout=25)
    if r.status_code != 200:
        print("Telegram gönderim hatası:", r.status_code, r.text)


# ===============================
# Utils
# ===============================
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1, p2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dl = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(p1)*cos(p2)*sin(dl/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


def iso_from_koeri(date_str: str, time_str: str) -> str:
    # KOERI: 2025.12.25 08:38:32
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y.%m.%d %H:%M:%S")
    # DB'ye UTC gibi yazıyoruz (KOERI saatleri TR olabilir; ama kıyaslar kendi içinde tutarlı)
    # İstersen burada timezone dönüşümü eklenebilir.
    return dt.replace(tzinfo=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_float_safe(x):
    try:
        return float(x)
    except:
        return None


def pick_magnitude(parts):
    """
    KOERI satırında genelde sütunlar:
    Tarih Saat Enlem Boylam Derinlik MD ML Mw Yer ... (bazı varyasyonlar)
    Biz güvenli şekilde 6-7-8. sütunlardan ilk sayısalı seçiyoruz.
    """
    # En azından: date time lat lon depth ... location
    # Magnitude genelde index 5/6/7 civarı.
    cand_idx = [5, 6, 7]  # esnek dene
    for i in cand_idx:
        if i < len(parts):
            v = parse_float_safe(parts[i])
            if v is not None:
                return v
    return None


# ===============================
# DB (migrate + init)
# ===============================
def ensure_db():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()

    # tablo yoksa oluştur
    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            latitude REAL,
            longitude REAL,
            depth REAL,
            magnitude REAL,
            location TEXT,
            source TEXT DEFAULT 'KOERI',
            UNIQUE(event_time, latitude, longitude, magnitude)
        )
    """)
    con.commit()

    # migrate: eski şemalardan gelebilecek kolonları kontrol et
    cols = [r[1] for r in cur.execute("PRAGMA table_info(earthquakes)").fetchall()]

    # Eğer eski kolon adları varsa ekle/uyumla (lat/lon -> latitude/longitude)
    if "lat" in cols and "latitude" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN latitude REAL")
        con.commit()
        cur.execute("UPDATE earthquakes SET latitude = lat WHERE latitude IS NULL")
        con.commit()

    if "lon" in cols and "longitude" not in cols:
        cur.execute("ALTER TABLE earthquakes ADD COLUMN longitude REAL")
        con.commit()
        cur.execute("UPDATE earthquakes SET longitude = lon WHERE longitude IS NULL")
        con.commit()

    # diğer olası kolon isimleri
    if "event_id" in cols and "id" not in cols:
        # sqlite'da id rename zor; gerek yok. sadece bilgilendirme.
        pass

    con.close()


def insert_event(event_row) -> int:
    """
    event_row: (event_time, latitude, longitude, depth, magnitude, location, source)
    """
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO earthquakes
            (event_time, latitude, longitude, depth, magnitude, location, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, event_row)
        con.commit()
        ins = cur.rowcount
    except Exception as e:
        print("DB hata:", e)
        ins = 0
    finally:
        con.close()
    return ins


def get_rows_since_iso(since_iso: str):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    rows = cur.execute("""
        SELECT event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
    """, (since_iso,)).fetchall()
    con.close()
    return rows


# ===============================
# Fetch KOERI
# ===============================
def fetch_koeri_500():
    html = requests.get(KOERI_URL, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        return []

    lines = pre.get_text("\n").splitlines()
    events = []

    for ln in lines:
        s = ln.strip()
        if not s or s.lower().startswith("tarih"):
            continue

        parts = s.split()
        if len(parts) < 6:
            continue

        date_str, time_str = parts[0], parts[1]
        lat = parse_float_safe(parts[2])
        lon = parse_float_safe(parts[3])
        depth = parse_float_safe(parts[4])
        mag = pick_magnitude(parts)

        if lat is None or lon is None or depth is None or mag is None:
            continue

        # location: genelde magnitude sütunlarından sonra kalan
        # En güvenlisi: son 1-2 sütun "Çözüm Niteliği" vb olabilir.
        # Biz tüm metni alıp; date time lat lon depth + 3 mag alanı gibi sabitleri düşürüp kalanları birleştiriyoruz.
        # Basit yaklaşım: ilk 8 sütundan sonrası.
        location = " ".join(parts[8:]) if len(parts) > 8 else ""

        event_time = iso_from_koeri(date_str, time_str)
        events.append((event_time, lat, lon, depth, mag, location, "KOERI"))

    return events


def koeri_latest_day(events):
    # KOERI en yeni tarihi: event_time ISO'dan YYYY-MM-DD
    if not events:
        return None
    latest = max(e[0] for e in events)
    return latest[:10]  # YYYY-MM-DD


# ===============================
# Alarm Mantığı
# ===============================
def filter_within_radius(rows, center_lat, center_lon, radius_km):
    out = []
    for (t, la, lo, dep, mag, loc) in rows:
        if la is None or lo is None:
            continue
        if haversine_km(center_lat, center_lon, la, lo) <= radius_km:
            out.append((t, la, lo, dep, mag, loc))
    return out


def bandirma_alarm(rows_30d, rows_14d):
    """
    Bandırma 70km:
    🟠 TURUNCU: Mw>=5.0, pencere 30 gün
    🔴 KIRMIZI: Mw>=5.5, pencere 7–14 gün (biz 14 gün aldık)
    """
    within_30 = filter_within_radius(rows_30d, BANDIRMA_LAT, BANDIRMA_LON, 70.0)
    within_14 = filter_within_radius(rows_14d, BANDIRMA_LAT, BANDIRMA_LON, 70.0)

    max30 = max([r[4] for r in within_30], default=None)
    max14 = max([r[4] for r in within_14], default=None)

    orange = (max30 is not None and max30 >= 5.0)
    red = (max14 is not None and max14 >= 5.5)

    return {
        "orange": orange,
        "red": red,
        "count30": len(within_30),
        "count14": len(within_14),
        "max30": max30,
        "max14": max14,
        "top30": within_30[:5],  # en yeni 5
        "top14": within_14[:5],
    }


def turkey_cluster_alarm(rows_30d, rows_7d, rows_24h):
    """
    Türkiye geneli 'küme alarmı' (senin ilk yazdığın kriter – 100km):
    🟠 Turuncu (Mw≥6 riski artışı) — 100 km:
      - 24h: M>=3.0 >=40 ve maxMag(24h) >=4.0
      - 7d : M>=3.0 >=25 ve M>=4.0 >=2
      - 30d: en az 1 adet M>=5.0

    🔴 Kırmızı (Mw≥7 riski / çok yüksek tehlike) — 100 km:
      - 7d : M>=6.5 >=1
      - 30d: M>=5.8 >=2
      - 24h: M>=4.0 >=10

    Burada "küme"yi pratik yapmak için şöyle ele alıyoruz:
    - Her depremin koordinatını merkez alıp 100km içinde sayımları yapıyoruz.
    - En güçlü (en yüksek risk) kümeyi raporluyoruz.
    """
    def counts_within(center_lat, center_lon, rows):
        return [r for r in rows if haversine_km(center_lat, center_lon, r[1], r[2]) <= 100.0]

    # merkez adayları: son 30g içindeki olaylar
    candidates = [(r[1], r[2]) for r in rows_30d if r[1] is not None and r[2] is not None]
    if not candidates:
        return {"orange": False, "red": False, "best": None}

    best = None

    for (clat, clon) in candidates[:200]:  # aşırı uzamasın diye limit
        c24 = counts_within(clat, clon, rows_24h)
        c7 = counts_within(clat, clon, rows_7d)
        c30 = counts_within(clat, clon, rows_30d)

        n24_m3 = sum(1 for r in c24 if r[4] >= 3.0)
        n24_m4 = sum(1 for r in c24 if r[4] >= 4.0)
        max24 = max([r[4] for r in c24], default=0.0)

        n7_m3 = sum(1 for r in c7 if r[4] >= 3.0)
        n7_m4 = sum(1 for r in c7 if r[4] >= 4.0)
        n7_m65 = sum(1 for r in c7 if r[4] >= 6.5)

        n30_m5 = sum(1 for r in c30 if r[4] >= 5.0)
        n30_m58 = sum(1 for r in c30 if r[4] >= 5.8)

        orange = (n24_m3 >= 40 and max24 >= 4.0) or (n7_m3 >= 25 and n7_m4 >= 2) or (n30_m5 >= 1)
        red = (n7_m65 >= 1) or (n30_m58 >= 2) or (n24_m4 >= 10)

        # skor: kırmızı > turuncu, sonra sayımlar
        score = (2 if red else 1 if orange else 0) * 10_000 + n24_m3 * 50 + n7_m3 * 10 + n30_m5 * 100

        if best is None or score > best["score"]:
            best = {
                "score": score,
                "center": (clat, clon),
                "orange": orange,
                "red": red,
                "n24_m3": n24_m3,
                "n24_m4": n24_m4,
                "max24": max24,
                "n7_m3": n7_m3,
                "n7_m4": n7_m4,
                "n7_m65": n7_m65,
                "n30_m5": n30_m5,
                "n30_m58": n30_m58,
                "sample": c24[:5],  # en yeni birkaç örnek
            }

    return {"orange": bool(best and best["orange"]), "red": bool(best and best["red"]), "best": best}


def fmt_event(e):
    t, la, lo, dep, mag, loc = e
    return f"• {t} | M{mag:.1f} | {loc}".strip()


# ===============================
# MAIN
# ===============================
def main():
    ensure_db()

    # 1) KOERI çek -> DB'ye ekle
    events = fetch_koeri_500()
    print(f"KOERI satır: {len(events)}")

    inserted = 0
    for ev in events:
        inserted += insert_event(ev)
    print("Yeni eklenen:", inserted)

    # 2) Pencereler (DB'den)
    now = datetime.now(timezone.utc)
    since_30 = (now - timedelta(days=30)).isoformat(timespec="seconds").replace("+00:00", "Z")
    since_14 = (now - timedelta(days=14)).isoformat(timespec="seconds").replace("+00:00", "Z")
    since_7 = (now - timedelta(days=7)).isoformat(timespec="seconds").replace("+00:00", "Z")
    since_1 = (now - timedelta(days=1)).isoformat(timespec="seconds").replace("+00:00", "Z")

    rows_30d = get_rows_since_iso(since_30)
    rows_14d = get_rows_since_iso(since_14)
    rows_7d = get_rows_since_iso(since_7)
    rows_24h = get_rows_since_iso(since_1)

    # 3) Alarm değerlendirme
    band = bandirma_alarm(rows_30d, rows_14d)
    tr = turkey_cluster_alarm(rows_30d, rows_7d, rows_24h)

    # 4) Mesaj (tek mesaj – iki başlık)
    hedef_gun = koeri_latest_day(events) or "N/A"

    msg = []
    msg.append("📌 <b>DEPREM ALARM RAPORU</b>")
    msg.append(f"🕒 Çalışma zamanı (UTC): {now.isoformat(timespec='seconds').replace('+00:00','Z')}")
    msg.append(f"📅 KOERI en yeni gün: <b>{hedef_gun}</b>")
    msg.append("")

    # Bandırma
    msg.append("🏠 <b>Bandırma (70 km) Alarm</b>")
    msg.append(f"• 30g kayıt: {band['count30']} | maxM(30g): {band['max30'] if band['max30'] is not None else 'Yok'}")
    msg.append(f"• 14g kayıt: {band['count14']} | maxM(14g): {band['max14'] if band['max14'] is not None else 'Yok'}")

    if band["red"]:
        msg.append("🔴 <b>KIRMIZI</b> (14g içinde M≥5.5 var)")
    elif band["orange"]:
        msg.append("🟠 <b>TURUNCU</b> (30g içinde M≥5.0 var)")
    else:
        msg.append("🟢 Normal (kriter yok)")

    # Türkiye küme
    msg.append("")
    msg.append("🇹🇷 <b>Türkiye Geneli Küme Alarmı (100 km)</b>")
    if tr["best"] is None:
        msg.append("• Veri yok / küme hesaplanamadı")
    else:
        b = tr["best"]
        clat, clon = b["center"]
        msg.append(f"• Küme merkezi: {clat:.4f}, {clon:.4f}")
        msg.append(f"• 24h: M≥3.0={b['n24_m3']} | M≥4.0={b['n24_m4']} | maxM={b['max24']:.1f}")
        msg.append(f"• 7g : M≥3.0={b['n7_m3']} | M≥4.0={b['n7_m4']} | M≥6.5={b['n7_m65']}")
        msg.append(f"• 30g: M≥5.0={b['n30_m5']} | M≥5.8={b['n30_m58']}")

        if b["red"]:
            msg.append("🔴 <b>KIRMIZI</b> (küme kriteri sağlandı)")
        elif b["orange"]:
            msg.append("🟠 <b>TURUNCU</b> (küme kriteri sağlandı)")
        else:
            msg.append("🟢 Normal (kriter yok)")

    # İsteğe bağlı: Bandırma en yeni 3 olay
    msg.append("")
    msg.append("🧾 <b>Bandırma 70km - En yeni 3 kayıt</b>")
    within_30 = filter_within_radius(rows_30d, BANDIRMA_LAT, BANDIRMA_LON, 70.0)
    for e in within_30[:3]:
        msg.append(fmt_event(e))
    if not within_30:
        msg.append("• Yok")

    # 5) Her çalışmada Telegram'a tek mesaj gönder
    telegram_send("\n".join(msg))
    print("Telegram rapor mesajı gönderildi (env varsa).")


if __name__ == "__main__":
    main()
