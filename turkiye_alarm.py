# turkiye_alarm.py
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from math import radians, sin, cos, asin, sqrt

# -----------------------------
# Helpers
# -----------------------------
def _to_dt_utc(s: str) -> datetime:
    """
    DB'deki event_time ISO string -> timezone-aware UTC datetime
    Kabul: '2025-12-26T12:34:56+00:00' veya '...Z'
    """
    if not s:
        return datetime.now(timezone.utc)
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))

def within_radius(rows, center_lat, center_lon, radius_km: float):
    """
    rows: list of dict {event_time, latitude, longitude, depth, magnitude, location, source}
    """
    out = []
    for r in rows:
        d = haversine_km(center_lat, center_lon, float(r["latitude"]), float(r["longitude"]))
        if d <= radius_km:
            rr = dict(r)
            rr["dist_km"] = d
            out.append(rr)
    return out

def fetch_rows(db_file: str, since_dt_utc: datetime):
    """
    since_dt_utc: timezone-aware UTC
    """
    since_iso = since_dt_utc.astimezone(timezone.utc).isoformat(timespec="seconds")
    con = sqlite3.connect(db_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        """
        SELECT event_id, event_time, latitude, longitude, depth, magnitude, location, source
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
        """,
        (since_iso,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def get_last_n_rows(db_file: str, n: int = 5):
    con = sqlite3.connect(db_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        """
        SELECT event_id, event_time, latitude, longitude, depth, magnitude, location, source
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT ?
        """,
        (int(n),),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def format_last5(rows_last5):
    lines = []
    lines.append("📌 Son 5 Deprem (DB)")
    for r in rows_last5:
        dt = _to_dt_utc(r["event_time"]).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        mag = r.get("magnitude", "")
        dep = r.get("depth", "")
        loc = (r.get("location") or "").strip()
        lat = r.get("latitude", "")
        lon = r.get("longitude", "")
        src = (r.get("source") or "").strip()
        lines.append(f"- {dt} | M{mag} | {dep}km | {loc} | ({lat},{lon}) | {src}")
    return "\n".join(lines)

# -----------------------------
# Alarm logic (Bandırma 70 km)
# -----------------------------
def build_report(db_file: str, center_lat: float, center_lon: float, radius_km: float = 70.0):
    """
    Returns: (has_alarm: bool, message: str, last5_block: str)
    """
    now_utc = datetime.now(timezone.utc)

    rows_24h = fetch_rows(db_file, now_utc - timedelta(days=1))
    rows_7d  = fetch_rows(db_file, now_utc - timedelta(days=7))
    rows_30d = fetch_rows(db_file, now_utc - timedelta(days=30))

    in_24h = within_radius(rows_24h, center_lat, center_lon, radius_km)
    in_7d  = within_radius(rows_7d,  center_lat, center_lon, radius_km)
    in_30d = within_radius(rows_30d, center_lat, center_lon, radius_km)

    # Basit ama stabil eşikler (istersen sonra geliştiririz):
    # - KIRMIZI: 70 km içinde son 24 saatte Mw>=5.5
    # - TURUNCU: 70 km içinde son 7 günde Mw>=5.0  (kırmızı yoksa)
    red = any(float(r["magnitude"]) >= 5.5 for r in in_24h)
    orange = (not red) and any(float(r["magnitude"]) >= 5.0 for r in in_7d)

    last5 = get_last_n_rows(db_file, 5)
    last5_block = format_last5(last5)

    if not (red or orange):
        return False, "Alarm yok.", last5_block

    level = "🔴 KIRMIZI" if red else "🟠 TURUNCU"
    msg = []
    msg.append(f"{level} ALARM")
    msg.append(f"📍 Merkez: {center_lat:.4f},{center_lon:.4f} | Yarıçap: {radius_km:.0f} km")
    msg.append("")

    # Alarmı tetikleyen en büyük olayı öne çıkar
    candidates = in_24h if red else in_7d
    candidates_sorted = sorted(candidates, key=lambda x: float(x["magnitude"]), reverse=True)
    top = candidates_sorted[0]
    dt = _to_dt_utc(top["event_time"]).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    msg.append("⭐ Tetikleyen olay:")
    msg.append(
        f"- {dt} | M{top['magnitude']} | {top['depth']}km | {top['location']} | "
        f"{top.get('dist_km',0):.1f} km"
    )
    msg.append("")
    msg.append(last5_block)

    return True, "\n".join(msg), last5_block

# Dışarıya tek “stabil” API verelim:
def turkiye_alarm(db_file: str, center_lat: float, center_lon: float, radius_km: float = 70.0):
    """
    main.py bununla konuşsun.
    Returns: (has_alarm: bool, message: str, last5_block: str)
    """
    return build_report(db_file, center_lat, center_lon, radius_km)
