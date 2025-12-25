# turkiye_alarm.py
# -*- coding: utf-8 -*-

import os
import math
import sqlite3
from datetime import datetime, timedelta, timezone


# ------------------------------------------------------------
# Datetime helpers (CRITICAL: naive vs aware problemini çözer)
# ------------------------------------------------------------
def parse_event_time_to_utc_naive(s: str) -> datetime:
    """
    DB'deki event_time genelde şu formatlarda gelir:
      - 2025-12-25T08:38:32Z
      - 2025-12-25T08:38:32+00:00
      - 2025-12-25 08:38:32   (nadiren)
    Biz hepsini UTC'ye çevirip tz'siz (naive) datetime döndürüyoruz.
    Böylece naive/aware karşılaştırma hatası biter.
    """
    if not s:
        return datetime(1970, 1, 1)

    s = s.strip()

    try:
        # ISO: "Z" -> "+00:00"
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            # fromisoformat hem "T" hem " " ayırıcıyı kabul eder
            dt = datetime.fromisoformat(s)
    except Exception:
        # son çare: "YYYY-MM-DD HH:MM:SS"
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime(1970, 1, 1)

    # dt aware ise UTC'ye çevirip tzinfo kaldır (naive UTC)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    # dt zaten naive ise "UTC naive" kabul ediyoruz
    return dt


def utc_now_naive() -> datetime:
    """UTC now (naive) - deprecated utcnow kullanmadan."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ------------------------------------------------------------
# Geo helpers
# ------------------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         (math.sin(dlon / 2) ** 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def within_radius(rows, center_lat: float, center_lon: float, radius_km: float):
    """
    rows: list[dict] beklenir: {"dt","lat","lon","depth","mag","loc"}
    """
    out = []
    for r in rows:
        d = haversine_km(center_lat, center_lon, r["lat"], r["lon"])
        if d <= radius_km:
            out.append(r)
    return out


# ------------------------------------------------------------
# DB
# ------------------------------------------------------------
def fetch_rows(db_file: str, since_dt: datetime):
    """
    DB kolonları (senin ekranda gördüğün):
    ['event_id','event_time','latitude','longitude','depth','magnitude','location','source']

    since_dt: naive UTC datetime
    """
    since_iso = since_dt.replace(microsecond=0).isoformat(timespec="seconds") + "Z"

    con = sqlite3.connect(db_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # event_time ISO string olduğu için string compare çalışıyor (Z ile sabit format)
    cur.execute(
        """
        SELECT event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
        """,
        (since_iso,)
    )

    rows = []
    for row in cur.fetchall():
        dt = parse_event_time_to_utc_naive(row["event_time"])
        rows.append({
            "dt": dt,
            "lat": float(row["latitude"]),
            "lon": float(row["longitude"]),
            "depth": float(row["depth"]) if row["depth"] is not None else None,
            "mag": float(row["magnitude"]) if row["magnitude"] is not None else None,
            "loc": (row["location"] or "").strip(),
        })

    con.close()
    return rows


# ------------------------------------------------------------
# Alarm logic
# ------------------------------------------------------------
def count_ge(rows, mag_threshold: float) -> int:
    return sum(1 for r in rows if (r["mag"] is not None and r["mag"] >= mag_threshold))


def max_mag(rows) -> float:
    mags = [r["mag"] for r in rows if r["mag"] is not None]
    return max(mags) if mags else 0.0


def format_top(rows, n=5) -> str:
    out = []
    for r in rows[:n]:
        out.append(f"- {r['dt'].strftime('%Y-%m-%d %H:%M:%S')} | M{r['mag']:.1f} | {r['loc']}")
    return "\n".join(out) if out else "- (yok)"


def bandirma_alarm(rows_24h, rows_7d, rows_30d):
    """
    Bandırma (yarıçap main.py’den gelir; sen 70 km istedin)
    🟠 Turuncu:
      - 24h: M>=3.0 >=40 ve maxMag(24h) >=4.0
      - 7d:  M>=3.0 >=25 ve M>=4.0 >=2
      - 30d: M>=5.0 >=1
    🔴 Kırmızı:
      - 7d:  M>=6.5 >=1
      - 30d: M>=5.8 >=2
      - 24h: M>=4.0 >=10
    """
    c24_m3 = count_ge(rows_24h, 3.0)
    c24_m4 = count_ge(rows_24h, 4.0)
    mx24 = max_mag(rows_24h)

    c7_m3 = count_ge(rows_7d, 3.0)
    c7_m4 = count_ge(rows_7d, 4.0)
    c7_m65 = count_ge(rows_7d, 6.5)

    c30_m5 = count_ge(rows_30d, 5.0)
    c30_m58 = count_ge(rows_30d, 5.8)

    orange = (
        (c24_m3 >= 40 and mx24 >= 4.0) or
        (c7_m3 >= 25 and c7_m4 >= 2) or
        (c30_m5 >= 1)
    )

    red = (
        (c7_m65 >= 1) or
        (c30_m58 >= 2) or
        (c24_m4 >= 10)
    )

    lines = []
    lines.append(f"24s: M>=3.0={c24_m3}, M>=4.0={c24_m4}, maxM24={mx24:.1f}")
    lines.append(f"7g : M>=3.0={c7_m3}, M>=4.0={c7_m4}, M>=6.5={c7_m65}")
    lines.append(f"30g: M>=5.0={c30_m5}, M>=5.8={c30_m58}")

    return orange, red, "\n".join(lines)


def find_best_cluster(all_rows_30d, radius_km: float, candidate_days: int):
    """
    Türkiye geneli "küme" yaklaşımı:
    - Son 30 günden aday merkezler seç (en büyük magnitüdlü ilk N olay)
    - Her aday için son candidate_days içinde yarıçapta maxM hesapla
    - En yüksek maxM olan merkezi döndür
    """
    if not all_rows_30d:
        return None

    now = utc_now_naive()
    since_candidates = now - timedelta(days=candidate_days)

    # CRITICAL: dt hepsi naive UTC olduğu için karşılaştırma güvenli
    recent = [r for r in all_rows_30d if r["dt"] >= since_candidates]
    if not recent:
        recent = all_rows_30d

    # en büyük M olanlardan aday merkezler
    candidates = sorted(recent, key=lambda r: (r["mag"] or 0.0), reverse=True)[:25]

    best = None
    best_mx = -1.0
    best_rows = None

    for c in candidates:
        clat, clon = c["lat"], c["lon"]
        in_rad = within_radius(all_rows_30d, clat, clon, radius_km)
        mx = max_mag(in_rad)
        if mx > best_mx:
            best_mx = mx
            best = (clat, clon)
            best_rows = sorted(in_rad, key=lambda r: r["dt"], reverse=True)

    return {
        "center": best,
        "maxM_30d": best_mx,
        "rows_30d": best_rows or []
    }


def turkey_alarm_cluster(all_rows_30d, radius_km: float):
    """
    Türkiye geneli küme alarmı (yarıçap 70):
    🟠 TURUNCU: hedef Mw>=5.0, pencere 30 gün
    🔴 KIRMIZI: hedef Mw>=5.5, pencere 7–14 gün (biz 14 gün alıyoruz)
    """
    if not all_rows_30d:
        return False, False, "TR küme: veri yok"

    cluster = find_best_cluster(all_rows_30d, radius_km=radius_km, candidate_days=14)
    if not cluster or not cluster["center"]:
        return False, False, "TR küme: merkez bulunamadı"

    clat, clon = cluster["center"]
    rows30 = cluster["rows_30d"]
    mx30 = max_mag(rows30)

    now = utc_now_naive()
    rows14 = [r for r in rows30 if r["dt"] >= (now - timedelta(days=14))]
    mx14 = max_mag(rows14)

    orange = (mx30 >= 5.0)
    red = (mx14 >= 5.5)

    msg = []
    msg.append(f"Merkez: {clat:.4f}, {clon:.4f} | Yarıçap: {radius_km:.0f} km")
    msg.append(f"maxM(30g)={mx30:.1f} | maxM(14g)={mx14:.1f}")
    msg.append("Son olaylar:")
    msg.append(format_top(sorted(rows30, key=lambda r: r['dt'], reverse=True), n=5))
    return orange, red, "\n".join(msg)


def build_report(db_file: str, bandirma_lat: float, bandirma_lon: float, radius_km: float = 70.0):
    """
    main.py burayı çağırıyor:
      has_alarm, msg = build_report(DB_FILE, BANDIRMA_LAT, BANDIRMA_LON, radius_km=70.0)
    """
    now = utc_now_naive()

    all_30d = fetch_rows(db_file, now - timedelta(days=30))
    all_7d = fetch_rows(db_file, now - timedelta(days=7))
    all_24h = fetch_rows(db_file, now - timedelta(hours=24))

    # Bandırma çevresi
    b_30d = within_radius(all_30d, bandirma_lat, bandirma_lon, radius_km)
    b_7d = within_radius(all_7d, bandirma_lat, bandirma_lon, radius_km)
    b_24h = within_radius(all_24h, bandirma_lat, bandirma_lon, radius_km)

    b_orange, b_red, b_stats = bandirma_alarm(b_24h, b_7d, b_30d)

    # Türkiye geneli küme (aynı yarıçapı kullan)
    tr_orange, tr_red, tr_info = turkey_alarm_cluster(all_30d, radius_km=radius_km)

    # Telegram mesajı
    msg = []
    msg.append("📌 **BANDIRMA (70 km) ALARM**")
    msg.append(("🔴 KIRMIZI" if b_red else ("🟠 TURUNCU" if b_orange else "🟢 NORMAL")))
    msg.append(b_stats)
    msg.append("Son Bandırma olayları:")
    msg.append(format_top(sorted(b_30d, key=lambda r: r["dt"], reverse=True), n=5))

    msg.append("\n📌 **TÜRKİYE GENELİ KÜME (70 km) ALARM**")
    msg.append(("🔴 KIRMIZI" if tr_red else ("🟠 TURUNCU" if tr_orange else "🟢 NORMAL")))
    msg.append(tr_info)

    has_alarm = (b_orange or b_red or tr_orange or tr_red)
    return has_alarm, "\n".join(msg)


# main.py importları bozulmasın diye:
def turkey_alarm(*args, **kwargs):
    # geriye uyum için alias
    return turkey_alarm_cluster(*args, **kwargs)
