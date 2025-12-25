import math
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

# =========================
# GEO
# =========================
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1 = math.radians(lat1); phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def iso_to_dt(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

# =========================
# DB row -> event dict
# =========================
def rows_to_events(db_rows: List[Tuple[str, float, float, float, float, str]]) -> List[Dict[str, Any]]:
    events = []
    for t, lat, lon, depth, mag, loc in db_rows:
        try:
            dt = iso_to_dt(t)
        except:
            continue
        try:
            events.append({
                "t": dt,
                "lat": float(lat),
                "lon": float(lon),
                "depth": float(depth) if depth is not None else None,
                "mag": float(mag) if mag is not None else None,
                "location": (loc or "").strip(),
            })
        except:
            continue
    events.sort(key=lambda x: x["t"], reverse=True)
    return events

# =========================
# TÜRKİYE GENELİ: Kümeleme
# =========================
def cluster_events(events: List[Dict[str, Any]], radius_km: float) -> List[Dict[str, Any]]:
    clusters: List[Dict[str, Any]] = []
    for ev in events:
        lat, lon = ev["lat"], ev["lon"]
        best_idx = None
        best_dist = 1e9

        for i, c in enumerate(clusters):
            d = haversine_km(lat, lon, c["center_lat"], c["center_lon"])
            if d <= radius_km and d < best_dist:
                best_dist = d
                best_idx = i

        if best_idx is None:
            clusters.append({"center_lat": lat, "center_lon": lon, "events": [ev]})
        else:
            c = clusters[best_idx]
            c["events"].append(ev)
            n = len(c["events"])
            c["center_lat"] = (c["center_lat"] * (n - 1) + lat) / n
            c["center_lon"] = (c["center_lon"] * (n - 1) + lon) / n

    return clusters

def _top_event(evs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not evs:
        return None
    return max(evs, key=lambda x: x["mag"] if x["mag"] is not None else -1)

def evaluate_turkiye_clusters(
    db_rows: List[Tuple[str, float, float, float, float, str]],
    radius_km: float = 70.0,
    orange_mag: float = 5.0,
    orange_days: int = 30,
    red_mag: float = 5.5,
    red_days: int = 14,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    if now is None:
        now = datetime.utcnow()

    events = rows_to_events(db_rows)
    clusters = cluster_events(events, radius_km=radius_km)

    orange_since = now - timedelta(days=orange_days)
    red_since = now - timedelta(days=red_days)

    red_list = []
    orange_list = []

    for c in clusters:
        evs = c["events"]
        orange_evs = [e for e in evs if e["t"] >= orange_since]
        red_evs = [e for e in evs if e["t"] >= red_since]

        orange_max = max([e["mag"] for e in orange_evs if e["mag"] is not None], default=None)
        red_max = max([e["mag"] for e in red_evs if e["mag"] is not None], default=None)

        orange_top = _top_event(orange_evs)
        red_top = _top_event(red_evs)

        summary = {
            "center": (c["center_lat"], c["center_lon"]),
            "count_30d": len(orange_evs),
            "count_redwin": len(red_evs),
            "orange_max": orange_max,
            "red_max": red_max,
            "orange_top": orange_top,  # {t, mag, location, ...}
            "red_top": red_top,
        }

        is_red = (red_max is not None) and (red_max >= red_mag)
        is_orange = (orange_max is not None) and (orange_max >= orange_mag)

        if is_red:
            red_list.append(summary)
        elif is_orange:
            orange_list.append(summary)

    red_list.sort(key=lambda x: (x["red_max"] or 0, x["count_redwin"]), reverse=True)
    orange_list.sort(key=lambda x: (x["orange_max"] or 0, x["count_30d"]), reverse=True)

    return {
        "now_utc": now,
        "radius_km": radius_km,
        "orange_days": orange_days,
        "orange_mag": orange_mag,
        "red_days": red_days,
        "red_mag": red_mag,
        "cluster_count_total": len(clusters),
        "red": red_list,
        "orange": orange_list,
    }

# =========================
# BANDIRMA 70 km: Senin kriterlerin
# =========================
def _filter_radius(events: List[Dict[str, Any]], center_lat: float, center_lon: float, radius_km: float):
    out = []
    for e in events:
        d = haversine_km(center_lat, center_lon, e["lat"], e["lon"])
        if d <= radius_km:
            out.append(e)
    return out

def _count_mag(evs: List[Dict[str, Any]], thr: float) -> int:
    return sum(1 for e in evs if (e["mag"] is not None and e["mag"] >= thr))

def _max_mag(evs: List[Dict[str, Any]]) -> Optional[float]:
    mags = [e["mag"] for e in evs if e["mag"] is not None]
    return max(mags) if mags else None

def evaluate_bandirma_alarm(
    db_rows: List[Tuple[str, float, float, float, float, str]],
    center_lat: float,
    center_lon: float,
    radius_km: float = 70.0,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    🟠 Turuncu (Mw≥6 riski artışı) — 70 km
      - 24h: M>=3.0 >= 40 AND maxMag(24h) >= 4.0
      - 7d : M>=3.0 >= 25 AND M>=4.0 >= 2
      - 30d: M>=5.0 >= 1

    🔴 Kırmızı (Mw≥7 riski / çok yüksek tehlike) — 70 km
      - 7d : M>=6.5 >= 1
      - 30d: M>=5.8 >= 2
      - 24h: M>=4.0 >= 10
    """
    if now is None:
        now = datetime.utcnow()

    events = rows_to_events(db_rows)
    local = _filter_radius(events, center_lat, center_lon, radius_km)

    t24 = now - timedelta(hours=24)
    t7d = now - timedelta(days=7)
    t30 = now - timedelta(days=30)

    ev24 = [e for e in local if e["t"] >= t24]
    ev7  = [e for e in local if e["t"] >= t7d]
    ev30 = [e for e in local if e["t"] >= t30]

    # metrikler
    c24_m3 = _count_mag(ev24, 3.0)
    c24_m4 = _count_mag(ev24, 4.0)
    max24  = _max_mag(ev24)

    c7_m3  = _count_mag(ev7, 3.0)
    c7_m4  = _count_mag(ev7, 4.0)
    c7_m65 = _count_mag(ev7, 6.5)

    c30_m5  = _count_mag(ev30, 5.0)
    c30_m58 = _count_mag(ev30, 5.8)

    # TURUNCU tetik
    orange_reasons = []
    if (c24_m3 >= 40) and (max24 is not None and max24 >= 4.0):
        orange_reasons.append("24s: M≥3.0 ≥40 ve maxMw(24s) ≥4.0")
    if (c7_m3 >= 25) and (c7_m4 >= 2):
        orange_reasons.append("7g: M≥3.0 ≥25 ve M≥4.0 ≥2")
    if c30_m5 >= 1:
        orange_reasons.append("30g: M≥5.0 ≥1")

    # KIRMIZI tetik
    red_reasons = []
    if c7_m65 >= 1:
        red_reasons.append("7g: M≥6.5 ≥1")
    if c30_m58 >= 2:
        red_reasons.append("30g: M≥5.8 ≥2")
    if c24_m4 >= 10:
        red_reasons.append("24s: M≥4.0 ≥10")

    red = len(red_reasons) > 0
    orange = (len(orange_reasons) > 0) and (not red)  # kırmızı varsa turuncuyu ayrı saymıyoruz

    top24 = _top_event(ev24)
    top7  = _top_event(ev7)
    top30 = _top_event(ev30)

    return {
        "now_utc": now,
        "center": (center_lat, center_lon),
        "radius_km": radius_km,
        "counts": {
            "24h_M>=3": c24_m3,
            "24h_M>=4": c24_m4,
            "24h_max": max24,
            "7d_M>=3": c7_m3,
            "7d_M>=4": c7_m4,
            "7d_M>=6.5": c7_m65,
            "30d_M>=5": c30_m5,
            "30d_M>=5.8": c30_m58,
        },
        "red": red,
        "orange": orange,
        "red_reasons": red_reasons,
        "orange_reasons": orange_reasons,
        "top": {
            "24h": top24,
            "7d": top7,
            "30d": top30,
        },
    }
