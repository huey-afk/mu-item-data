import re, json, datetime
import requests
import pytds

ITEM_URL = "https://raw.githubusercontent.com/huey-afk/mu-item-data/main/Item.txt"

SQL_HOST = "15.235.181.212"
SQL_PORT = 1433
SQL_DB   = "MuOnlineFinals"
SQL_USER = "sa"
SQL_PASS = "@Phowns2019"  # change this

SQL_QUERY = """
SELECT AccountID, ItemIndex, ItemLevel, ItemCount
FROM dbo.CustomItemBank;
"""

# Make stricter if you want only specific jewels
JEWEL_KEYWORDS = ("jewel",)

def index_to_group_type(idx: int):
    return idx // 512, idx % 512

def load_item_names_from_url(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    text = r.text

    group_re = re.compile(r'^\s*(\d+)\s*$')
    row_re = re.compile(r'^\s*(\d+)\s+.*?"([^"]+)"')

    cur_group = None
    mapping = {}

    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("//"):
            continue

        mg = group_re.match(s)
        if mg:
            cur_group = int(mg.group(1))
            continue

        if cur_group is None:
            continue

        mr = row_re.match(line)
        if mr:
            itype = int(mr.group(1))
            name = mr.group(2).strip()
            mapping[(cur_group, itype)] = name

    return mapping

def fetch_rows_pytds():
    # Note: autocommit True to avoid transaction hassles
    with pytds.connect(
        server=SQL_HOST,
        port=SQL_PORT,
        database=SQL_DB,
        user=SQL_USER,
        password=SQL_PASS,
        autocommit=True,
        timeout=10
    ) as conn:
        cur = conn.cursor()
        cur.execute(SQL_QUERY)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

def is_jewel(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in JEWEL_KEYWORDS)

def main():
    print("📥 Loading Item.txt...")
    item_map = load_item_names_from_url(ITEM_URL)
    print("✅ Items loaded:", len(item_map))

    print("🗄️ Fetching CustomItemBank...")
    rows = fetch_rows_pytds()
    print("✅ Bank rows:", len(rows))

    out = []
    unknown = 0

    for r in rows:
        idx = int(r["ItemIndex"])
        group, itype = index_to_group_type(idx)
        name = item_map.get((group, itype), "UNKNOWN_ITEM")
        if name == "UNKNOWN_ITEM":
            unknown += 1



        out.append({
            "AccountID": str(r["AccountID"]).strip(),
            "ItemIndex": idx,
            "Group": group,
            "Type": itype,
            "ItemName": name,
            "ItemLevel": int(r["ItemLevel"]),
            "ItemCount": int(r["ItemCount"]),
        })

    out.sort(key=lambda x: (x["ItemIndex"], x["AccountID"]))

    payload = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "unknown_mappings": unknown,
        "rows": out
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    print("✅ Wrote data.json rows:", len(out))
    print("❓ Unknown item mappings:", unknown)

if __name__ == "__main__":
    main()
