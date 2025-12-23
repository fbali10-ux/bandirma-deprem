# main.py (v2 - regex ile sağlam parse)
# KOERI'den veri çek -> SQLite'a ekle (mükerrer imkansız) -> 50.000 kayıt limiti -> analiz
# Windows 10 + Python 3.13 uyumlu

import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import hashlib
import math
import re
from typing import List, Dict, Tuple, Optional

KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst9.asp"

DB_PATH = "deprem.db"
MAX_ROWS = 50_000

# Bandırma merkez (yaklaşık)
BANDIRMA_LAT = 40.3520
BANDIRMA_LON = 27.9700
BANDIRMA_RADIUS_KM = 100.0

# Alarm eşikleri (istersen sonra netleştiririz)
ORANGE_HOURS = 24
ORANGE_MIN_MAG = 4.5
ORANGE_MIN_COUNT = 3

RED_HOURS = 48
RED_MIN_MAG = 5.5
RED_MIN_COUNT = 2
RED_SINGLE_MAG = 6.0

# Türkiye geneli kümeleşme (basit)
CLUSTER_LOOKBACK_HOURS = 48
CLUSTER_MIN_MAG = 4.0
CLUSTER_DISTANCE_KM = 50.0
CLUSTER_MIN_COUNT = 4


# Örnek satırlar farklı olabiliyor; regex ile yakalıyoruz:
# 2025.12.23 08:05:12 40.1234 27.1234 10.5 3.2 ... yer
DT_RE = re.compile(r"(\d{4}\.\d{2}\.\d{2})\s+(\d{2}:\d{2}:\d{2})")
# Enlem/Boylam KOERI'de genelde 2 ondalık+; negatif olmaz ama olur diye destek:
LATLON_RE = re.compile(r"(-?\d{2}\.\d+)\s+(-?\d{2}\.\d+)")
# Derinlik genelde 0-700 arası float:
DEPTH_RE = re.compile(r"\s(-?\d{1,3}\.\d+|-?\d{1,3})\s")
# Magnitüd genelde 0.0-9.9:
MAG_RE = re.compile(r"(?<!\d)(\d\.\d)(?!\d)")  # 1.2, 3.4 gibi


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS earthquakes (
        event_id TEXT PRIMARY KEY,
        event_time TEXT NOT NULL,   -- ISO UTC
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        depth REAL,
        magnitude REAL,
        location TEXT,
        source TEXT
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_eq_time ON earthquakes(event_time);")
    conn.commit()


def to_utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_koeri_datetime(date_s: str, time_s: str) -> datetime:
    # KOERI saati TR (UTC+3) kabul edip UTC'ye çeviriyoruz
    dt = datetime.strptime(f"{date_s} {time_s}", "%Y.%m.%d %H:%M:%S")
    tr = timezone(timedelta(hours=3))
    return dt.replace(tzinfo=tr).astimezone(timezone.utc)


def make_event_id(event_time_utc_iso: str, lat: float, lon: float, depth: float, mag: float, loc: str) -> str:
    raw = f"{event_time_utc_iso}|{lat:.4f}|{lon:.4f}|{depth:.1f}|{mag:.1f}|{(loc or '').strip()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def try_parse_line(line: str) -> Optional[Dict]:
    ln = " ".join(line.split())
    if not ln:
        return None
    # başlık/ayraç satırları
    if "Tarih" in ln and "Saat" in ln:
        return None
    if set(ln) <= set("-"):
        return None

    mdt = DT_RE.search(ln)
    if not mdt:
        return None
    date_s, time_s = mdt.group(1), mdt.group(2)

    mll = LATLON_RE.search(ln)
    if not mll:
        return None
    lat = float(mll.group(1))
    lon = float(mll.group(2))

    # tarih+saatten sonra gelen sayılardan depth ve mag yakalamaya çalışacağız
    # Strategy:
    # - önce dt ve latlon parçalarını kes
    idx = mll.end()
    tail = ln[idx:].strip()

    # tail içindeki ilk "muhtemel" depth sayısını al (0-700 civarı)
    depth = None
    # basit: tail'deki ilk sayı
    nums = re.findall(r"-?\d+(?:\.\d+)?", tail)
    # nums içinde lat/lon yok, tail zaten latlon sonrası; ilk sayı genelde depth olur
    if len(nums) >= 1:
        try:
            depth = float(nums[0])
        except Exception:
            depth = None

    # magnitude: KOERI satırlarında genelde derinlikten sonra 1-2 farklı mag var.
    # pratik: tail'deki 2. veya 3. sayı mag olur; ama güvenli olsun diye 0.0-9.9 arası alıyoruz.
    mag = None
    cand = []
    for x in nums[:6]:
        try:
            fx = float(x)
            if 0.0 <= fx <= 9.9:
                cand.append(fx)
        except Exception:
            pass

    # cand[0] çoğu zaman depth olur (örn 10.5) ama 0-9.9 filtresi depth'i de içeri alabilir.
    # depth 0-9.9 ise karışır. Bu yüzden:
    # - depth'i ayrıca aldık
    # - mag için: cand içinde depth'e en yakın olmayan ve 0.0-9.9 olan birini seçmeye çalış
    # pratik kural: mag genelde 1.x-6.x; depth çoğu zaman 5-20. İkisi çakışırsa sonrakini al.
    mag_candidates = [v for v in cand if v != (depth if depth is not None else -999)]
    if mag_candidates:
        # satırda genelde mag1, mag2 olur; en büyüğü daha anlamlı
        mag = max(mag_candidates)
    else:
        # fallback: satırda "x.y" ara
        mm = MAG_RE.findall(ln)
        if mm:
            mag = max(float(v) for v in mm)

    # yer bilgisi: satırın sonunda çoğunlukla parantezli gelir
    # Heuristik: tail içinde son ")" sonrası yoksa tüm tail'i yer say
    location = ""
    # sayıları temizle, geriye kalan metin
    location_guess = re.sub(r"-?\d+(?:\.\d+)?", " ", tail)
    location_guess = " ".join(location_guess.split())
    if location_guess:
        location = location_guess

    # datetime
    dt_utc = parse_koeri_datetime(date_s, time_s)
    event_time_iso = to_utc_iso(dt_utc)

    if mag is None:
        return None
    if depth is None:
        depth = 0.0

    event_id = make_event_id(event_time_iso, lat, lon, float(depth), float(mag), location)

    return {
        "event_id": event_id,
        "event_time": event_time_iso,
        "latitude": lat,
        "longitude": lon,
        "depth": float(depth),
        "magnitude": float(mag),
        "location": location,
        "source": "KOERI"
    }


def fetch_koeri_rows() -> Tuple[List[Dict], Dict]:
    r = requests.get(KOERI_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "html.parser")
    pre = soup.find("pre")
    if not pre:
        raise RuntimeError("KOERI sayfasında <pre> bulunamadı. Sayfa formatı değişmiş olabilir.")

    lines = [ln.rstrip("\n") for ln in pre.get_text("\n").splitlines()]

    stats = {"lines_total": len(lines), "lines_nonempty": 0, "lines_parsed": 0, "lines_skipped": 0}

    rows: List[Dict] = []
    for ln in lines:
        if ln.strip():
            stats["lines_nonempty"] += 1
        parsed = try_parse_line(ln)
        if parsed:
            rows.append(parsed)
            stats["lines_parsed"] += 1
        else:
            stats["lines_skipped"] += 1

    return rows, stats


def upsert_rows(conn: sqlite3.Connection, rows: List[Dict]) -> Tuple[int, int]:
    inserted = 0
    skipped = 0
    cur = conn.cursor()
    for row in rows:
        try:
            cur.execute("""
                INSERT INTO earthquakes(event_id, event_time, latitude, longitude, depth, magnitude, location, source)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["event_id"], row["event_time"], row["latitude"], row["longitude"],
                row["depth"], row["magnitude"], row["location"], row["source"]
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    return inserted, skipped


def prune_to_max(conn: sqlite3.Connection, max_rows: int = MAX_ROWS) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM earthquakes;")
    total = cur.fetchone()[0]
    if total <= max_rows:
        return 0

    to_delete = total - max_rows
    cur.execute("""
        SELECT event_id FROM earthquakes
        ORDER BY event_time ASC
        LIMIT ?
    """, (to_delete,))
    ids = [r[0] for r in cur.fetchall()]
    cur.executemany("DELETE FROM earthquakes WHERE event_id = ?;", [(i,) for i in ids])
    conn.commit()
    return len(ids)


def load_recent(conn: sqlite3.Connection, since_utc: datetime) -> List[Dict]:
    since_iso = to_utc_iso(since_utc)
    cur = conn.cursor()
    cur.execute("""
        SELECT event_id, event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        WHERE event_time >= ?
        ORDER BY event_time DESC
    """, (since_iso,))
    out = []
    for r in cur.fetchall():
        out.append({
            "event_id": r[0],
            "event_time": r[1],
            "latitude": r[2],
            "longitude": r[3],
            "depth": r[4],
            "magnitude": r[5],
            "location": r[6] or ""
        })
    return out


class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))
        self.sz = [1]*n

    def find(self, a: int) -> int:
        while self.p[a] != a:
            self.p[a] = self.p[self.p[a]]
            a = self.p[a]
        return a

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.sz[ra] < self.sz[rb]:
            ra, rb = rb, ra
        self.p[rb] = ra
        self.sz[ra] += self.sz[rb]


def detect_clusters(events: List[Dict]) -> List[List[Dict]]:
    n = len(events)
    if n == 0:
        return []

    dsu = DSU(n)
    for i in range(n):
        for j in range(i+1, n):
            di = haversine_km(events[i]["latitude"], events[i]["longitude"],
                              events[j]["latitude"], events[j]["longitude"])
            if di <= CLUSTER_DISTANCE_KM:
                dsu.union(i, j)

    groups = {}
    for i in range(n):
        root = dsu.find(i)
        groups.setdefault(root, []).append(events[i])

    clusters = []
    for g in groups.values():
        if len(g) >= CLUSTER_MIN_COUNT:
            g_sorted = sorted(g, key=lambda x: (x["magnitude"], x["event_time"]), reverse=True)
            clusters.append(g_sorted)

    clusters.sort(key=lambda g: max(e["magnitude"] for e in g), reverse=True)
    return clusters


def bandirma_alarm(events_recent: List[Dict]) -> Dict:
    local = []
    for e in events_recent:
        d = haversine_km(BANDIRMA_LAT, BANDIRMA_LON, e["latitude"], e["longitude"])
        if d <= BANDIRMA_RADIUS_KM:
            e2 = dict(e)
            e2["distance_km"] = d
            local.append(e2)

    now_utc = datetime.now(timezone.utc)
    orange_since = now_utc - timedelta(hours=ORANGE_HOURS)
    red_since = now_utc - timedelta(hours=RED_HOURS)

    def parse_iso(s: str) -> datetime:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    orange_hits = [e for e in local if e["magnitude"] >= ORANGE_MIN_MAG and parse_iso(e["event_time"]) >= orange_since]
    red_hits = [e for e in local if e["magnitude"] >= RED_MIN_MAG and parse_iso(e["event_time"]) >= red_since]
    red_single = [e for e in local if e["magnitude"] >= RED_SINGLE_MAG and parse_iso(e["event_time"]) >= red_since]

    alarm = {"level": "YOK", "orange_hits": orange_hits, "red_hits": red_hits, "red_single": red_single, "local_all": local}

    if len(red_single) >= 1 or len(red_hits) >= RED_MIN_COUNT:
        alarm["level"] = "KIRMIZI"
    elif len(orange_hits) >= ORANGE_MIN_COUNT:
        alarm["level"] = "TURUNCU"

    return alarm


def print_report(stats: Dict, inserted: int, skipped: int, pruned: int, alarm: Dict, clusters: List[List[Dict]]) -> None:
    print("\n" + "="*70)
    print("KOERI VERİ ÇEKME + ANALİZ RAPORU (v2)")
    print("="*70)
    print(f"KOERI satır sayısı (toplam): {stats['lines_total']}")
    print(f"KOERI satır sayısı (boş olmayan): {stats['lines_nonempty']}")
    print(f"KOERI parse edilen satır: {stats['lines_parsed']}")
    print(f"KOERI atlanan satır: {stats['lines_skipped']}")

    print(f"\nYeni eklenen kayıt: {inserted}")
    print(f"Mükerrer (atlanmış): {skipped}")
    if pruned:
        print(f"50.000 limit için silinen (en eski): {pruned}")
    else:
        print("50.000 limit: OK (silme yok)")

    print("\n--- Bandırma 100 km Analizi ---")
    print(f"Toplam yerel kayıt (son 48 saat): {len(alarm['local_all'])}")
    print(f"Alarm seviyesi: {alarm['level']}")

    if alarm["level"] == "TURUNCU":
        print(f"Turuncu tetikleyen olay sayısı (son {ORANGE_HOURS} saat, M>={ORANGE_MIN_MAG}): {len(alarm['orange_hits'])}")
        for e in sorted(alarm["orange_hits"], key=lambda x: x["event_time"], reverse=True)[:10]:
            print(f"  - {e['event_time']} | M{e['magnitude']:.1f} | {e['distance_km']:.1f} km | {e['location']}")
    elif alarm["level"] == "KIRMIZI":
        print(f"Kırmızı (son {RED_HOURS} saat) M>={RED_MIN_MAG} sayısı: {len(alarm['red_hits'])}")
        if alarm["red_single"]:
            print(f"Kırmızı tekil (M>={RED_SINGLE_MAG}) sayısı: {len(alarm['red_single'])}")
        hits = alarm["red_single"] if alarm["red_single"] else alarm["red_hits"]
        for e in sorted(hits, key=lambda x: x["event_time"], reverse=True)[:10]:
            d = e.get("distance_km", haversine_km(BANDIRMA_LAT, BANDIRMA_LON, e["latitude"], e["longitude"]))
            print(f"  - {e['event_time']} | M{e['magnitude']:.1f} | {d:.1f} km | {e['location']}")
    else:
        if alarm["local_all"]:
            top = sorted(alarm["local_all"], key=lambda x: x["magnitude"], reverse=True)[:5]
            print("Son 48 saatteki en büyük 5 yerel kayıt:")
            for e in top:
                print(f"  - {e['event_time']} | M{e['magnitude']:.1f} | {e['distance_km']:.1f} km | {e['location']}")
        else:
            print("Bandırma 100 km içinde son 48 saatte kayıt görünmüyor.")

    print("\n--- Türkiye Geneli Kümeleşme (basit) ---")
    if not clusters:
        print(f"Son {CLUSTER_LOOKBACK_HOURS} saatte (M>={CLUSTER_MIN_MAG}) belirgin kümeleşme yok.")
    else:
        print(f"Kümeleşme bulundu: {len(clusters)} küme (eşik: {CLUSTER_DISTANCE_KM} km, min adet: {CLUSTER_MIN_COUNT})")
        for idx, cl in enumerate(clusters[:3], start=1):
            maxmag = max(e["magnitude"] for e in cl)
            print(f"\nKüme #{idx} | Olay sayısı: {len(cl)} | Max M: {maxmag:.1f}")
            for e in cl[:8]:
                print(f"  - {e['event_time']} | M{e['magnitude']:.1f} | {e['location']}")

    print("\nNot: Telegram'ı sonra ekleyeceğiz. Şu an sadece konsola rapor basıyor.")
    print("="*70 + "\n")


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_db(conn)

        rows, stats = fetch_koeri_rows()
        inserted, skipped = upsert_rows(conn, rows)
        pruned = prune_to_max(conn, MAX_ROWS)

        now_utc = datetime.now(timezone.utc)
        recent_48h = load_recent(conn, now_utc - timedelta(hours=48))

        alarm = bandirma_alarm(recent_48h)

        tr_candidates = [e for e in recent_48h if e["magnitude"] >= CLUSTER_MIN_MAG]
        clusters = detect_clusters(tr_candidates)

        print_report(stats, inserted, skipped, pruned, alarm, clusters)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
