import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

DB_FILE = "deprem.db"
KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst6.asp"

def fetch_koeri_lines():
    r = requests.get(KOERI_URL, timeout=30)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    pre = soup.find("pre")
    if not pre:
        return []
    lines = [ln.rstrip("\n") for ln in pre.get_text("\n").splitlines()]
    out = []
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        if s.startswith("Tarih") or "SON DEPREMLER" in s or s.startswith("----") or s.startswith(".."):
            continue
        parts = s.split()
        if len(parts) < 10:
            continue
        # parts[0]=YYYY.MM.DD, parts[1]=HH:MM:SS
        out.append(parts)
    return out

def koeri_max_day_and_counts(parts_list):
    # KOERI'deki en yeni günü bul (hardcode yok)
    max_dt = None
    for p in parts_list:
        try:
            dt = datetime.strptime(f"{p[0]} {p[1]}", "%Y.%m.%d %H:%M:%S")
            if (max_dt is None) or (dt > max_dt):
                max_dt = dt
        except:
            pass

    if not max_dt:
        return None, 0, 0, []

    target_day = max_dt.strftime("%Y-%m-%d")
    # O güne ait satır sayısı
    day_count = 0
    for p in parts_list:
        if p[0].replace(".", "-") == target_day:
            day_count += 1

    # KOERI en son 5 satır (ekranda görmek için)
    last5 = []
    # KOERI listesi zaten genelde yeniler üstte; yine de dt’ye göre sıralayalım:
    def to_dt(p):
        return datetime.strptime(f"{p[0]} {p[1]}", "%Y.%m.%d %H:%M:%S")
    parts_sorted = sorted(parts_list, key=to_dt, reverse=True)
    for p in parts_sorted[:5]:
        # Ekrana daha okunur bas
        last5.append(" ".join(p))

    return target_day, len(parts_list), day_count, last5

def db_counts(target_day):
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()

    # tablo var mı?
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = [t[0] for t in tables]
    if "earthquakes" not in tables:
        con.close()
        return 0, None, []

    total = cur.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
    max_time = cur.execute("SELECT MAX(event_time) FROM earthquakes").fetchone()[0]

    # event_time "YYYY-MM-DDTHH:MM:SS"
    day_count = cur.execute(
        "SELECT COUNT(*) FROM earthquakes WHERE substr(event_time,1,10)=?",
        (target_day,)
    ).fetchone()[0]

    last5 = cur.execute("""
        SELECT event_time, latitude, longitude, depth, magnitude, location
        FROM earthquakes
        ORDER BY event_time DESC
        LIMIT 5
    """).fetchall()

    con.close()
    return total, max_time, day_count, last5

def main():
    print("=== KOERI vs DB KONTROL ===")
    print(f"DB: {DB_FILE}")
    print(f"KOERI: {KOERI_URL}\n")

    parts_list = fetch_koeri_lines()
    if not parts_list:
        print("KOERI parse edilemedi (<pre> yok ya da format değişti).")
        return

    target_day, koeri_total, koeri_day_count, koeri_last5 = koeri_max_day_and_counts(parts_list)
    if not target_day:
        print("KOERI'den tarih çıkarılamadı.")
        return

    db_total, db_max_time, db_day_count, db_last5 = db_counts(target_day)

    print(f"Hedef gün (KOERI en yeni gün): {target_day}\n")

    print(f"DB toplam kayıt: {db_total}")
    print(f"DB en yeni event_time: {db_max_time}")
    print(f"DB {target_day} kayıt: {db_day_count}\n")

    print(f"KOERI toplam satır (parse edilen): {koeri_total}")
    print(f"KOERI {target_day} satır: {koeri_day_count}\n")

    diff = koeri_day_count - db_day_count
    if diff == 0:
        print("✅ DB ile KOERI aynı (hedef gün satır sayısı eşit).")
    elif diff > 0:
        print(f"⚠️ KOERI'de {diff} adet daha fazla satır var (DB geride olabilir).")
    else:
        print(f"⚠️ DB, KOERI'den {-diff} adet fazla görünüyor (parse/filtre farkı olabilir).")

    print("\n--- DB SON 5 (event_time DESC) ---")
    for r in db_last5:
        print(r)

    print("\n--- KOERI SON 5 (en yeni 5 satır) ---")
    for ln in koeri_last5:
        print(ln)

if __name__ == "__main__":
    main()
