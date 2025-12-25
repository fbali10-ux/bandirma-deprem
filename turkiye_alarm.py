import os
import sqlite3
from datetime import datetime, timedelta, timezone
from math import radians, sin, cos, sqrt, atan2


# -----------------------------
# Helpers
# -----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    # SQLite'da TEXT isoformat saklıyoruz
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def within_radius(rows, center_lat: float, center_lon: float, radius_km: float):
    out = []
    for r in rows:
        d = haversine_km(center_lat, center_lon, r["latitude"], r["longitude"])
        if d <= radius_km:
            out.append(r)
    return out


def _parse_iso(dt_str: str) -> datetime:
    # event_time isoformat (UTC) bekliyoruz
    # örn: 2025-12-25T08:38:32+00:00
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        # fallback: Z yoksa
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


# -----------------------------
# DB fetch (LAT/LON değil: latitude/longitude)
# -----------------------------
def fetch_rows(db_file: str, since_dt: datetime):
    since_iso = iso(since_dt)
    con = sqlite3.connect(db_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute(
        """
        SELECT event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
        """,
        (since_iso,),
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            {
                "event_time": row["event_time"],
                "dt": _parse_iso(row["event_time"]),
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "depth": float(row["depth"]) if row["depth"] is not None else None,
                "magnitude": float(row["magnitude"]) if row["magnitude"] is not None else None,
                "location": row["location"] or "",
            }
        )
    con.close()
    return rows


# -----------------------------
# Alarm logic
# -----------------------------
def _count_ge(rows, mag: float) -> int:
    return sum(1 for r in rows if (r["magnitude"] is not None and r["magnitude"] >= mag))


def _max_mag(rows) -> float:
    mags = [r["magnitude"] for r in rows if r["magnitude"] is not None]
    return max(mags) if mags else 0.0


def evaluate_cluster_criteria(rows_24h, rows_7d, rows_30d):
    """
    Kullanıcının verdiği kriterler (yarıçap dışarıdan seçilecek):
    🟠 Turuncu:
      - 24h: M>=3.0 >=40 AND maxMag>=4.0
      - 7d:  M>=3.0 >=25 AND M>=4.0 >=2
      - 30d: M>=5.0 >=1
    🔴 Kırmızı:
      - 7d:  M>=6.5 >=1
      - 30d: M>=5.8 >=2
      - 24h: M>=4.0 >=10
    """
    c24_m3 = _count_ge(rows_24h, 3.0)
    c24_m4 = _count_ge(rows_24h, 4.0)
    max24 = _max_mag(rows_24h)

    c7_m3 = _count_ge(rows_7d, 3.0)
    c7_m4 = _count_ge(rows_7d, 4.0)
    c7_m65 = _count_ge(rows_7d, 6.5)

    c30_m5 = _count_ge(rows_30d, 5.0)
    c30_m58 = _count_ge(rows_30d, 5.8)

    # KIRMIZI
    red = (c7_m65 >= 1) or (c30_m58 >= 2) or (c24_m4 >= 10)

    # TURUNCU
    orange = ((c24_m3 >= 40) and (max24 >= 4.0)) or ((c7_m3 >= 25) and (c7_m4 >= 2)) or (c30_m5 >= 1)

    detail = {
        "c24_m3": c24_m3,
        "c24_m4": c24_m4,
        "max24": max24,
        "c7_m3": c7_m3,
        "c7_m4": c7_m4,
        "c7_m65": c7_m65,
        "c30_m5": c30_m5,
        "c30_m58": c30_m58,
    }
    return red, orange, detail


def find_best_cluster(all_rows_30d, radius_km: float, candidate_days: int = 7):
    """
    Türkiye geneli: "küme"yi aramak için
    son candidate_days içindeki olayları merkez adayı yapıp (lat/lon),
    o merkezin çevresinde 24h/7d/30d kriterlerini kontrol eder.
    """
    now = utc_now()
    since_24h = now - timedelta(hours=24)
    since_7d = now - timedelta(days=7)
    since_30d = now - timedelta(days=30)
    since_candidates = now - timedelta(days=candidate_days)

    rows_candidates = [r for r in all_rows_30d if r["dt"] >= since_candidates]
    if not rows_candidates:
        return None

    # performans: sadece daha anlamlı adayları alalım (M>=3 veya son 7 gün)
    # çok büyük DB'lerde çarpan azaltır
    rows_candidates = [r for r in rows_candidates if (r["magnitude"] is not None and r["magnitude"] >= 3.0)] or rows_candidates

    best = None  # dict
    # en fazla 250 adayla sınırlayalım (yeter)
    for cand in rows_candidates[:250]:
        clat, clon = cand["latitude"], cand["longitude"]

        rows_24h = [r for r in all_rows_30d if r["dt"] >= since_24h]
        rows_7d = [r for r in all_rows_30d if r["dt"] >= since_7d]
        rows_30d = [r for r in all_rows_30d if r["dt"] >= since_30d]

        w24 = within_radius(rows_24h, clat, clon, radius_km)
        w7 = within_radius(rows_7d, clat, clon, radius_km)
        w30 = within_radius(rows_30d, clat, clon, radius_km)

        red, orange, detail = evaluate_cluster_criteria(w24, w7, w30)
        if not (red or orange):
            continue

        sev = 2 if red else 1
        score = sev * 1_000_000 + int(detail["c24_m3"]) * 1000 + int(detail["c7_m3"])  # basit skor

        if (best is None) or (score > best["score"]):
            best = {
                "score": score,
                "severity": "RED" if red else "ORANGE",
                "center_lat": clat,
                "center_lon": clon,
                "detail": detail,
                "sample_location": cand["location"],
                "top_events_24h": sorted(w24, key=lambda x: (x["magnitude"] or 0), reverse=True)[:5],
            }

    return best


def bandirma_magnitude_alarm(all_rows_30d, center_lat, center_lon, radius_km: float):
    """
    Bandırma yerel ikinci kriter:
    🟠 TURUNCU: 30 gün, Mw>=5.0 (hedef) => 70 km içinde >=1
    🔴 KIRMIZI: 7–14 gün, Mw>=5.5 => 70 km içinde >=1 (burada 14 gün aldık)
    """
    now = utc_now()
    rows_14d = [r for r in all_rows_30d if r["dt"] >= (now - timedelta(days=14))]
    rows_30d = [r for r in all_rows_30d if r["dt"] >= (now - timedelta(days=30))]

    w14 = within_radius(rows_14d, center_lat, center_lon, radius_km)
    w30 = within_radius(rows_30d, center_lat, center_lon, radius_km)

    red = _count_ge(w14, 5.5) >= 1
    orange = _count_ge(w30, 5.0) >= 1

    detail = {
        "w14_m55": _count_ge(w14, 5.5),
        "w30_m50": _count_ge(w30, 5.0),
        "max14": _max_mag(w14),
        "max30": _max_mag(w30),
    }

    top = sorted(w30, key=lambda x: (x["magnitude"] or 0), reverse=True)[:5]
    return red, orange, detail, top


def build_report(db_file: str, bandirma_lat: float, bandirma_lon: float, radius_km: float = 70.0):
    now = utc_now()
    rows_30d = fetch_rows(db_file, now - timedelta(days=30))

    # 1) Türkiye geneli küme alarmı (kriter set 1)
    best_cluster = find_best_cluster(rows_30d, radius_km=radius_km, candidate_days=7)

    # 2) Bandırma 70 km yerel alarm (kriter set 2)
    b_red, b_orange, b_detail, b_top = bandirma_magnitude_alarm(
        rows_30d, bandirma_lat, bandirma_lon, radius_km=radius_km
    )

    has_alarm = False
    lines = []
    lines.append("🛰️ <b>Deprem Alarm Özeti</b>")
    lines.append(f"🕒 UTC: {iso(now)}")
    lines.append("")

    # --- Türkiye Cluster
    lines.append("🇹🇷 <b>Türkiye Geneli Küme Alarmı</b>")
    lines.append(f"📏 Yarıçap: {radius_km:.0f} km (küme arama)")
    if best_cluster is None:
        lines.append("✅ Kriterleri sağlayan küme yok.")
    else:
        sev = best_cluster["severity"]
        has_alarm = True
        emoji = "🔴" if sev == "RED" else "🟠"
        d = best_cluster["detail"]
        lines.append(f"{emoji} <b>{'KIRMIZI' if sev=='RED' else 'TURUNCU'}</b> (küme tespit)")
        lines.append(f"🎯 Merkez: {best_cluster['center_lat']:.4f}, {best_cluster['center_lon']:.4f}")
        if best_cluster.get("sample_location"):
            lines.append(f"📍 Örnek yer: {best_cluster['sample_location']}")
        lines.append(
            f"24h: M≥3.0={d['c24_m3']} | M≥4.0={d['c24_m4']} | maxMag={d['max24']:.1f}"
        )
        lines.append(
            f"7d:  M≥3.0={d['c7_m3']} | M≥4.0={d['c7_m4']} | M≥6.5={d['c7_m65']}"
        )
        lines.append(
            f"30d: M≥5.0={d['c30_m5']} | M≥5.8={d['c30_m58']}"
        )
        if best_cluster["top_events_24h"]:
            lines.append("Son 24h (en büyük 5):")
            for e in best_cluster["top_events_24h"]:
                lines.append(f" • Mw {e['magnitude']:.1f} | {e['event_time']} | {e['location']}")
    lines.append("")

    # --- Bandırma Local
    lines.append("📍 <b>Bandırma 70 km Yerel Alarm</b>")
    lines.append(f"🎯 Merkez: {bandirma_lat:.4f}, {bandirma_lon:.4f} | 📏 {radius_km:.0f} km")
    # Kırmızı öncelik
    if b_red:
        has_alarm = True
        lines.append("🔴 <b>KIRMIZI</b> (14g içinde Mw≥5.5 tespit)")
    elif b_orange:
        has_alarm = True
        lines.append("🟠 <b>TURUNCU</b> (30g içinde Mw≥5.0 tespit)")
    else:
        lines.append("✅ Kriter yok.")
    lines.append(f"14g: Mw≥5.5 = {b_detail['w14_m55']} | max={b_detail['max14']:.1f}")
    lines.append(f"30g: Mw≥5.0 = {b_detail['w30_m50']} | max={b_detail['max30']:.1f}")

    if b_top:
        lines.append("Son 30g (en büyük 5):")
        for e in b_top:
            lines.append(f" • Mw {e['magnitude']:.1f} | {e['event_time']} | {e['location']}")

    msg = "\n".join(lines)
    return has_alarm, msg
