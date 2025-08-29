import csv, json, pathlib
from google.cloud import storage

# ----- client -----
def storage_client():
    return storage.Client()

# ----- DAILY keys (flat filenames per asset) -----
def daily_csv_key(exchange: str, asset: str, day_iso: str) -> str:
    # coinbase/ADA/ADA-2025-08-28.csv
    return f"{exchange}/{asset}/{asset}-{day_iso}.csv"

def daily_json_key(exchange: str, asset: str, day_iso: str) -> str:
    # coinbase/ADA/ADA-2025-08-28.json
    return f"{exchange}/{asset}/{asset}-{day_iso}.json"

# ----- local file helpers -----
def local_daily_csv_path(base_dir: str | pathlib.Path, exchange: str, asset: str, day_iso: str) -> pathlib.Path:
    # data/coinbase/ADA-2025-08-28_coinbase.csv (local only)
    p = pathlib.Path(base_dir) / exchange / f"{asset}-{day_iso}_{exchange}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def ensure_csv_header(path: pathlib.Path):
    if not path.exists():
        with path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "timestamp","exchange","asset",
                "price","best_bid","best_ask","spread_raw",
                "spread_L5_pct","spread_L20_pct","spread_L50_pct","spread_L100_pct",
                "bid_volume_L50","ask_volume_L50"
            ])

# ----- bucket ops -----
def blob_exists(bucket, key: str) -> bool:
    return bucket.blob(key).exists()

def download_blob_to_file(bucket, key: str, local_path: pathlib.Path):
    b = bucket.blob(key)
    data = b.download_as_bytes()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with local_path.open("wb") as f:
        f.write(data)

def upload_file(bucket, key: str, local_path: pathlib.Path, content_type: str = "text/csv", cache_control: str = "no-cache, max-age=0"):
    b = bucket.blob(key)
    b.cache_control = cache_control
    b.upload_from_filename(str(local_path), content_type=content_type)

def upload_json(bucket, key: str, obj) -> None:
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    b = bucket.blob(key)
    b.cache_control = "no-cache, max-age=0"
    b.upload_from_string(payload, content_type="application/json")

def load_json_if_exists(bucket, key: str):
    b = bucket.blob(key)
    if not b.exists(): return None
    data = b.download_as_bytes()
    return json.loads(data.decode("utf-8"))
