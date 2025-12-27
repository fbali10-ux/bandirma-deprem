# -*- coding: utf-8 -*-
import os
import re
import math
import sqlite3
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup


# ==========================
# Ayarlar
# ==========================
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst6.asp"  # son ~500 satır
DB_PATH = "deprem.db"

# Bandırma merkez (yaklaşık)
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9767"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "100"))
BANDIRMA_LAST_N = int(os.getenv("BANDIRMA_LAST_N", "5"))

# Alarm eşikleri (birleşik skor)
# Varsayılanlar makul başlangıç; istersen optimize ederiz.
ALARM_ORANGE_THRESHOLD = float(os.getenv("ALARM_ORANGE_THRESHOLD", "6.0"))
ALARM_RED_THRESHOLD = float(os.getenv("ALARM_RED_THRESHOLD", "10.0"))

# Hızlı tetik (yakın zamanda büyük deprem olursa direkt)
# Örn: Bandırma 100 km içinde 24 saatte >=4.5 → ORANGE, >=5.0 → RED
ORANGE_QUICK_MAG = float(os.getenv("ORANGE_QUICK_MAG", "4.5"))
RED_QUICK_MAG = float(os.getenv("RED_QUICK_MAG", "5.0"))
QUICK_HOURS = int(os.getenv("QUICK_HOURS", "24"))

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Mesaj kontrol
FORCE_TELEGRAM = os.getenv("FORCE_TELEGRAM", "0") == "1"   # test için
ALWAYS_TELEGRAM = os.getenv("ALWAYS_TELEGRAM", "0") == "1" # istersen her çalışmada mesaj

HTTP_TIMEOUT = 30


# ==========================
# Yardımcılar
# ==========================
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p = math.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1 * p) * math.cos(lon2 * 0 + lat1 * 0 + 0)  # no-op to avoid lint
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def tz_tr():
    return timezone(timedelta(hours=3))


def now_tr() -> datetime:
    return datetime.now(tz_tr())


def fmt_event_time_tr(event_time_utc_iso: str) -> str:
    try:
        dt_utc = datetime.fromisoformat(event_time_utc_iso.replace("Z", "+00:00"))
        dt_tr = dt_utc.astimezone(tz_tr())
        return dt_tr.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return event_time_utc_iso


def parse_koeri_lst6() -> list[dict]:
    """
    KOERI lst6.asp <pre> içinden satırları parse eder.
    Çıktı: [{event_time(UTC ISO), latitude, longitude, depth_km, magnitude, location}, ...]
    """
    html = requests.get(KOERI_URL, timeout=HTTP_TIMEOUT).content
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı. Sayfa formatı değişmiş olabilir.")

    raw_lines = [ln.strip() for ln in pre.get_text("\n").splitlines() if ln.strip()]

    events = []
    for ln in raw_lines:
        if ln.lower().startswith("tarih") or ln.startswith("----") or "Date" in ln:
            continue

        parts = ln.split()
        if len(parts) < 9:
            continue

        date_s, time_s = parts[0], parts[1]
        if not re.match(r"^\d{4}\.\d{2}\.\d{2}$", date_s):
            continue
        if not re.match(r"^\d{2}:\d{2}:\d{2}$", time_s):
            continue

        try:
            lat = float(parts[2])
            lon = float(parts[3])
            depth_km = float(parts[4])
        except Exception:
            continue

        mag = None
        for idx in range(5, min(len(parts), 12)):
            try:
                x = float(parts[idx])
                if 0.0 <= x <= 10.0:
                    mag = x
                    break
            except Exception:
                pass
        if mag is None:
            continue

        location = " ".join(parts[8:]).strip() if len(parts) > 8 else "-"
        if not location:
            location = "-"

        # KOERI saatini TR kabul edip UTC’ye çevir
        try:
            dt_tr = datetime.strptime(f"{date_s} {time_s}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=tz_tr())
            dt_utc = dt_tr.astimezone(timezone.utc)
            event_time_utc_iso = dt_utc.isoformat()
        except Exception:
            continue

        events.append(
            dict(
                event_time=event_time_utc_iso,
                latitude=lat,
                longitude=lon,
                depth_km=depth_km,
                magnitude=mag,
                location=location,
            )
        )

    return events


# ==========================
# DB
# ==========================
def _table_exists(cur, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def _get_columns(cur, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def init_db(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    if not _table_exists(cur, "earthquakes"):
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS earthquakes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL,
                magnitude REAL,
                location TEXT
            )
            """
        )
        con.commit()
    else:
        cols = _get_columns(cur, "earthquakes")

        # depth_km yoksa ekle
        if "depth_km" not in cols:
            cur.execute("ALTER TABLE earthquakes ADD COLUMN depth_km REAL")
            con.commit()

            # Eski tabloda depth varsa kopyala
            cols2 = _get_columns(cur, "earthquakes")
            if "depth" in cols2:
                cur.execute("UPDATE earthquakes SET depth_km = depth WHERE depth_km IS NULL")
                con.commit()

        # magnitude / location yoksa ekle
        cols = _get_columns(cur, "earthquakes")
        if "magnitude" not in cols:
            cur.execute("ALTER TABLE earthquakes ADD COLUMN magnitude REAL")
            con.commit()
        if "location" not in cols:
            cur.execute("ALTER TABLE earthquakes ADD COLUMN location TEXT")
            con.commit()

    # UNIQUE index kurmadan önce olası kopyaları temizle (tek seferlik)
    # Aynı anahtarın birden fazla kaydı varsa en küçük id kalsın
    cur.execute(
        """
        DELETE FROM earthquakes
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM earthquakes
            GROUP BY event_time, latitude, longitude, IFNULL(magnitude,0), IFNULL(depth_km,0), IFNULL(location,'-')
        )
        """
    )
    con.commit()

    # Unique index
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_quake
        ON earthquakes (event_time, latitude, longitude, magnitude, depth_km, location)
        """
    )
    con.commit()
    con.close()


def upsert_events(db_path: str, events: list[dict]) -> int:
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    inserted = 0
    for e in events:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO earthquakes
                (event_time, latitude, longitude, depth_km, magnitude, location)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    e["event_time"],
                    float(e["latitude"]),
                    float(e["longitude"]),
                    float(e.get("depth_km") or 0.0),
                    float(e.get("magnitude") or 0.0),
                    str(e.get("location") or "-"),
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
        except Exception:
            continue

    con.commit()
    con.close()
    return inserted


def get_last_nearby(db_path: str, center_lat: float, center_lon: float, radius_km: float, limit: int) -> list[tuple]:
    """
    DB’den son kayıtları çekip merkez noktaya göre filtreler.
    Dönen: (event_time, mag, depth_km, location, dist_km)
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        """
        SELECT event_time, latitude, longitude, depth_km, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT 12000
        """
    )
    rows = cur.fetchall()
    con.close()

    out = []
    for event_time, lat, lon, depth_km, mag, location in rows:
        try:
            d = haversine_km(center_lat, center_lon, float(lat), float(lon))
        except Exception:
            continue
        if d <= radius_km:
            out.append((event_time, float(mag or 0.0), float(depth_km or 0.0), str(location or "-"), d))
            if len(out) >= limit:
                break
    return out


def fetch_bandirma_events_for_window(db_path: str, days: int) -> list[tuple]:
    """
    Son 'days' gün içinde Bandırma yarıçapına düşen kayıtları getirir.
    Dönen: (event_time, mag, depth_km, location, dist_km)
    """
    since_utc = datetime.now(timezone.utc) - timedelta(days=days)
    since_iso = since_utc.isoformat()

    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        """
        SELECT event_time, latitude, longitude, depth_km, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
        """,
        (since_iso,),
    )
    rows = cur.fetchall()
    con.close()

    out = []
    for event_time, lat, lon, depth_km, mag, location in rows:
        try:
            d = haversine_km(BANDIRMA_LAT, BANDIRMA_LON, float(lat), float(lon))
        except Exception:
            continue
        if d <= BANDIRMA_RADIUS_KM:
            out.append((event_time, float(mag or 0.0), float(depth_km or 0.0), str(location or "-"), d))
    return out


# ==========================
# Alarm (birleşik skor)
# ==========================
def event_weight(mag: float) -> float:
    """
    Küçük depremler düşük, büyük depremler yüksek katkı verir.
    mag 1.5 -> ~1.0
    mag 3.0 -> ~2.5
    mag 5.0 -> ~4.5
    """
    if mag <= 0:
        return 0.0
    base = 1.0
    extra = max(mag - 1.5, 0.0)
    return base + extra


def compute_alarm_status(db_path: str) -> dict:
    """
    90g + 30g + 7g + 1g pencerelerinden skor çıkarır.
    Quick trigger: son QUICK_HOURS içinde mag eşiği aşarsa direkt ORANGE/RED.
    """
    # Quick trigger (son N saat)
    quick_days = max(1, int(math.ceil(QUICK_HOURS / 24.0)))
    recent = fetch_bandirma_events_for_window(db_path, quick_days)
    # sadece son QUICK_HOURS saat içindekiler
    cutoff = datetime.now(timezone.utc) - timedelta(hours=QUICK_HOURS)
    recent_in_hours = []
    for (t, mag, dep, loc, dist) in recent:
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt >= cutoff:
            recent_in_hours.append((t, mag, dep, loc, dist))

    max_mag_quick = max([x[1] for x in recent_in_hours], default=0.0)
    quick_trigger = None
    if max_mag_quick >= RED_QUICK_MAG:
        quick_trigger = "RED"
    elif max_mag_quick >= ORANGE_QUICK_MAG:
        quick_trigger = "ORANGE"

    # Birleşik skor (90/30/7/1)
    windows = [90, 30, 7, 1]
    window_scores = {}
    for w in windows:
        evs = fetch_bandirma_events_for_window(db_path, w)
        # skor: toplam ağırlık / sqrt(gün) (uzun pencerede şişmesin)
        total_w = sum(event_weight(m) for (_, m, *_rest) in evs)
        score = total_w / math.sqrt(max(w, 1))
        window_scores[w] = {"count": len(evs), "total_w": total_w, "score": score}

    combined_score = sum(window_scores[w]["score"] for w in windows)

    # Alarm seviyesi
    level = "NORMAL"
    if combined_score >= ALARM_RED_THRESHOLD:
        level = "RED"
    elif combined_score >= ALARM_ORANGE_THRESHOLD:
        level = "ORANGE"

    # Quick trigger varsa yükselt
    if quick_trigger == "RED":
        level = "RED"
    elif quick_trigger == "ORANGE" and level != "RED":
        level = "ORANGE"

    return {
        "level": level,
        "combined_score": combined_score,
        "window_scores": window_scores,
        "max_mag_quick": max_mag_quick,
        "quick_trigger": quick_trigger,
        "quick_hours": QUICK_HOURS,
    }


# ==========================
# Telegram
# ==========================
def telegram_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram ENV eksik (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Mesaj atlanıyor.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"Telegram gönderim hatası: {r.status_code} {r.text[:300]}")
    except Exception as e:
        print(f"Telegram gönderim hatası: {e}")


def build_message(inserted: int) -> str:
    alarm = compute_alarm_status(DB_PATH)
    lastn = get_last_nearby(DB_PATH, BANDIRMA_LAT, BANDIRMA_LON, BANDIRMA_RADIUS_KM, BANDIRMA_LAST_N)

    # Alarm emoji
    if alarm["level"] == "RED":
        alarm_emoji = "🟥"
    elif alarm["level"] == "ORANGE":
        alarm_emoji = "🟧"
    else:
        alarm_emoji = "🟩"

    lines = []
    lines.append("📍 Bandırma Deprem Özeti")
    lines.append(f"🕒 Zaman (TR): {now_tr().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"🧾 Yeni eklenen kayıt: {inserted}")
    lines.append("")

    # Alarm durumu
    lines.append(f"{alarm_emoji} Alarm Durumu: {alarm['level']}")
    lines.append(f"📊 Birleşik Skor (90+30+7+1): {alarm['combined_score']:.2f}  |  Eşikler: ORANGE≥{ALARM_ORANGE_THRESHOLD}, RED≥{ALARM_RED_THRESHOLD}")
    if alarm["quick_trigger"]:
        lines.append(f"⚡ Hızlı Tetik: {alarm['quick_trigger']} (son {alarm['quick_hours']} saatte max Mw={alarm['max_mag_quick']:.1f})")
    else:
        lines.append(f"⚡ Hızlı Tetik: Yok (son {alarm['quick_hours']} saatte max Mw={alarm['max_mag_quick']:.1f})")

    ws = alarm["window_scores"]
    lines.append(
        f"🧮 Pencereler: 90g(score={ws[90]['score']:.2f}, adet={ws[90]['count']}) | 30g({ws[30]['score']:.2f},{ws[30]['count']}) | 7g({ws[7]['score']:.2f},{ws[7]['count']}) | 1g({ws[1]['score']:.2f},{ws[1]['count']})"
    )

    lines.append("")
    lines.append(f"📌 Bandırma ({int(BANDIRMA_RADIUS_KM)} km) - Son {BANDIRMA_LAST_N} Deprem")

    if not lastn:
        lines.append("— Kayıt bulunamadı.")
    else:
        for (event_time, mag, depth_km, location, dist_km) in lastn:
            t_tr = fmt_event_time_tr(event_time)
            lines.append(f"• Mw {mag:.1f} | {dist_km:.0f} km | {t_tr} | {location}")

    return "\n".join(lines)


def main():
    init_db(DB_PATH)

    events = parse_koeri_lst6()
    inserted = upsert_events(DB_PATH, events)

    print(f"KOERI parse: {len(events)} | Yeni eklenen: {inserted}")

    # Mesaj politikası
    # - yeni kayıt varsa mesaj
    # - FORCE_TELEGRAM=1 test için mesaj
    # - ALWAYS_TELEGRAM=1 her çalışmada mesaj
    if inserted > 0 or FORCE_TELEGRAM or ALWAYS_TELEGRAM:
        telegram_send(build_message(inserted))


if __name__ == "__main__":
    main()
