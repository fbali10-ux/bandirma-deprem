# turkiye_alarm.py
# -*- coding: utf-8 -*-

import os
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_FILE = os.getenv("DB_FILE", "deprem.db")

# Bandırma merkez koordinatı (ister env'den, ister default)
BANDIRMA_LAT = float(os.getenv("BANDIRMA_LAT", "40.3522"))
BANDIRMA_LON = float(os.getenv("BANDIRMA_LON", "27.9767"))
BANDIRMA_RADIUS_KM = float(os.getenv("BANDIRMA_RADIUS_KM", "70"))

# Türkiye geneli küme alarmı (sende 100 km gibi geçen kriterleri "Türkiye geneli" yapıyoruz)
# Türkiye geneli için radius kullanmıyoruz (ülke çapı), sadece zaman pencereleri & sayımlar.

# =========================
# ZAMAN / PARSE
# =========================
def parse_iso_dt(s: str) -> datetime:
    """
    DB'deki event_time genelde ISO: 2025-12-25T08:38:32 veya 2025-12-25 08:38:32 gibi olabilir.
    Hepsini UTC varsayarak datetime(UTC) döndürür.
    """
    if not s:
        return None
    s = s.strip()
    # Bazı kayıtlar "Z" ile bitebilir
    if s.endswith("Z"):
        s = s[:-1]
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # fallback: "YYYY-mm-dd HH:MM:SS"
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    # timezone yoksa UTC varsay
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# =========================
# COĞRAFYA
# =========================
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# =========================
# DB OKUMA
# =========================
def get_rows_since(days: int):
    """
    Son X gün kayıtlarını DB'den çeker.
    Kolonlar: event_time, latitude, longitude, depth, magnitude, location, source
    """
    since_dt = utcnow() - timedelta(days=days)
    since_iso = since_dt.isoformat(timespec="seconds")

    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()

    # Bazı DB'lerde source olmayabilir diye güvenli seçiyoruz:
    # Önce kolon var mı kontrol et.
    cols = [r[1] for r in cur.execute("PRAGMA table_info(earthquakes)").fetchall()]
    has_source = "source" in cols

    if has_source:
        q = """
            SELECT event_time, latitude, longitude, depth, magnitude, location, source
            FROM earthquakes
            WHERE event_time >= ?
            ORDER BY event_time DESC
        """
    else:
        q = """
            SELECT event_time, latitude, longitude, depth, magnitude, location
            FROM earthquakes
            WHERE event_time >= ?
            ORDER BY event_time DESC
        """

    rows = cur.execute(q, (since_iso,)).fetchall()
    con.close()

    out = []
    for r in rows:
        if has_source:
            event_time, lat, lon, depth, mag, loc, src = r
        else:
            event_time, lat, lon, depth, mag, loc = r
            src = None

        dt = parse_iso_dt(event_time)
        if dt is None:
            continue

        try:
            lat = float(lat)
            lon = float(lon)
            depth = float(depth) if depth is not None else None
            mag = float(mag) if mag is not None else None
        except Exception:
            continue

        out.append({
            "dt": dt,
            "event_time": event_time,
            "latitude": lat,
            "longitude": lon,
            "depth": depth,
            "magnitude": mag,
            "location": loc or "",
            "source": src or ""
        })

    return out


# =========================
# TÜRKİYE GENELİ ALARM (KÜME)
# =========================
def turkey_cluster_alert(rows_30d):
    """
    Kullanıcının verdiği Türkiye geneli kriterler (100 km gibi geçenler burada "ülke geneli küme" mantığı):
    🟠 Turuncu (Mw≥6 riski artışı) — 100 km
      - Son 24 saatte M≥3.0 ≥ 40 ve maxMag(24h) ≥ 4.0
      - Son 7 günde M≥3.0 ≥ 25 ve M≥4.0 ≥ 2
      - Son 30 günde en az 1 adet M≥5.0
    🔴 Kırmızı (Mw≥7 riski / çok yüksek tehlike) — 100 km
      - Son 7 günde M≥6.5 ≥ 1
      - Son 30 günde M≥5.8 ≥ 2
      - Son 24 saatte M≥4.0 ≥ 10

    Not: Burada radius bazlı clustering yok; ülke genelinde "aktivite artışı" şeklinde uygulanır.
    (İstersen sonraki adımda gerçek kümeleşmeyi grid/DBSCAN ile ekleriz.)
    """

    now = utcnow()
    r24 = [x for x in rows_30d if x["dt"] >= now - timedelta(days=1)]
    r7 = [x for x in rows_30d if x["dt"] >= now - timedelta(days=7)]
    r30 = rows_30d

    # Sayımlar
    m3_24 = sum(1 for x in r24 if x["magnitude"] is not None and x["magnitude"] >= 3.0)
    max_24 = max([x["magnitude"] for x in r24 if x["magnitude"] is not None], default=0.0)
    m3_7 = sum(1 for x in r7 if x["magnitude"] is not None and x["magnitude"] >= 3.0)
    m4_7 = sum(1 for x in r7 if x["magnitude"] is not None and x["magnitude"] >= 4.0)
    m5_30 = sum(1 for x in r30 if x["magnitude"] is not None and x["magnitude"] >= 5.0)

    m65_7 = sum(1 for x in r7 if x["magnitude"] is not None and x["magnitude"] >= 6.5)
    m58_30 = sum(1 for x in r30 if x["magnitude"] is not None and x["magnitude"] >= 5.8)
    m4_24 = sum(1 for x in r24 if x["magnitude"] is not None and x["magnitude"] >= 4.0)

    # Turuncu kriterleri
    orange_reasons = []
    if (m3_24 >= 40) and (max_24 >= 4.0):
        orange_reasons.append(f"24s: M≥3.0={m3_24} ve maxMag(24s)={max_24:.1f}")
    if (m3_7 >= 25) and (m4_7 >= 2):
        orange_reasons.append(f"7g: M≥3.0={m3_7} ve M≥4.0={m4_7}")
    if (m5_30 >= 1):
        orange_reasons.append(f"30g: M≥5.0={m5_30}")

    # Kırmızı kriterleri
    red_reasons = []
    if m65_7 >= 1:
        red_reasons.append(f"7g: M≥6.5={m65_7}")
    if m58_30 >= 2:
        red_reasons.append(f"30g: M≥5.8={m58_30}")
    if m4_24 >= 10:
        red_reasons.append(f"24s: M≥4.0={m4_24}")

    status = "YOK"
    if red_reasons:
        status = "KIRMIZI"
    elif orange_reasons:
        status = "TURUNCU"

    stats = {
        "m3_24": m3_24, "max_24": max_24,
        "m3_7": m3_7, "m4_7": m4_7,
        "m5_30": m5_30,
        "m65_7": m65_7, "m58_30": m58_30, "m4_24": m4_24
    }

    return status, orange_reasons, red_reasons, stats


# =========================
# BANDIRMA 70km ALARM
# =========================
def bandirma_alert(rows_30d, radius_km=70.0):
    """
    Kullanıcının istediği Bandırma (70km):
    🟠 TURUNCU:
      Hedef: Mw ≥5.0
      Pencere: 30 gün
    🔴 KIRMIZI:
      Hedef: Mw ≥5.5
      Pencere: 7–14 gün  (biz 14 gün alıyoruz; istersen env ile ayırırız)

    Not: Senin daha eski "100 km / M≥3 sayıları" kriterlerin Bandırma için ayrıca istenirse eklenir.
    Şu an bu basit hedef/pencere yaklaşımını uygular.
    """
    now = utcnow()

    # 30 gün, 14 gün, 7 gün
    r30 = rows_30d
    r14 = [x for x in rows_30d if x["dt"] >= now - timedelta(days=14)]

    def in_radius(x):
        return haversine_km(BANDIRMA_LAT, BANDIRMA_LON, x["latitude"], x["longitude"]) <= radius_km

    r30_in = [x for x in r30 if in_radius(x)]
    r14_in = [x for x in r14 if in_radius(x)]

    # turuncu: 30g içinde M>=5.0
    orange_hits = [x for x in r30_in if x["magnitude"] is not None and x["magnitude"] >= 5.0]

    # kırmızı: 14g içinde M>=5.5
    red_hits = [x for x in r14_in if x["magnitude"] is not None and x["magnitude"] >= 5.5]

    status = "YOK"
    if red_hits:
        status = "KIRMIZI"
    elif orange_hits:
        status = "TURUNCU"

    # En büyük olayları seç (özet için)
    orange_top = sorted(orange_hits, key=lambda x: x["magnitude"], reverse=True)[:5]
    red_top = sorted(red_hits, key=lambda x: x["magnitude"], reverse=True)[:5]

    return status, orange_top, red_top, {
        "count_30_in": len(r30_in),
        "count_14_in": len(r14_in),
        "orange_hits": len(orange_hits),
        "red_hits": len(red_hits),
    }


# =========================
# TEK MESAJ ÜRET
# =========================
def build_message():
    rows_30d = get_rows_since(30)

    # Türkiye geneli cluster
    tr_status, tr_orange, tr_red, tr_stats = turkey_cluster_alert(rows_30d)

    # Bandırma
    b_status, b_orange_top, b_red_top, b_stats = bandirma_alert(rows_30d, radius_km=BANDIRMA_RADIUS_KM)

    # Mesaj
    lines = []
    lines.append("📌 <b>DEPREM DURUM RAPORU</b>")
    lines.append(f"🕒 UTC: {utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # Türkiye geneli
    lines.append("🇹🇷 <b>TÜRKİYE GENELİ KÜME ALARMI</b>")
    if tr_status == "KIRMIZI":
        lines.append("🔴 <b>KIRMIZI</b>")
        for r in tr_red:
            lines.append(f"• {r}")
    elif tr_status == "TURUNCU":
        lines.append("🟠 <b>TURUNCU</b>")
        for r in tr_orange:
            lines.append(f"• {r}")
    else:
        lines.append("✅ Alarm yok")

    # küçük istatistik satırı
    lines.append(
        f"📊 24s(M≥3)={tr_stats['m3_24']}, max24={tr_stats['max_24']:.1f}, "
        f"7g(M≥3)={tr_stats['m3_7']}, 7g(M≥4)={tr_stats['m4_7']}, 30g(M≥5)={tr_stats['m5_30']}"
    )
    lines.append("")

    # Bandırma
    lines.append(f"📍 <b>BANDIRMA ({int(BANDIRMA_RADIUS_KM)} km)</b>")
    if b_status == "KIRMIZI":
        lines.append("🔴 <b>KIRMIZI</b> (14g içinde M≥5.5)")
        for x in b_red_top:
            t = x["dt"].strftime("%Y-%m-%d %H:%M")
            m = x["magnitude"]
            loc = x["location"]
            lines.append(f"• {t} | M{m:.1f} | {loc}")
    elif b_status == "TURUNCU":
        lines.append("🟠 <b>TURUNCU</b> (30g içinde M≥5.0)")
        for x in b_orange_top:
            t = x["dt"].strftime("%Y-%m-%d %H:%M")
            m = x["magnitude"]
            loc = x["location"]
            lines.append(f"• {t} | M{m:.1f} | {loc}")
    else:
        lines.append("✅ Alarm yok")

    lines.append(
        f"📊 30g kayıt(70km)={b_stats['count_30_in']}, 14g kayıt(70km)={b_stats['count_14_in']}, "
        f"turuncu_hit={b_stats['orange_hits']}, kırmızı_hit={b_stats['red_hits']}"
    )

    return "\n".join(lines), tr_status, b_status


# Dışarıdan main.py bunu çağırabilir:
def get_alert_summary():
    """
    (message, tr_status, bandirma_status)
    """
    return build_message()


if __name__ == "__main__":
    msg, tr_s, b_s = build_message()
    print(msg)
