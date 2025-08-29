import csv, json, pathlib, datetime as dt
from google.cloud import storage

# ----- client -----
def storage_client():
    return storage.Client()

# ----- key helpers -----
def hourly_csv_key(exchange: str, asset: str, day_iso: str, hour: int) -> str:
    return f"{exchange}/{asset}/{day_iso}/seconds_H{hour:02d}.csv"

def daily_csv_key(exchange: str, asset: str, day_iso: str) -> str:
    return f"{exchange}/{asset}/{day_iso}/daily.csv"

def daily_json_key(exchange: str, asset: str, day_iso: str) -> str:
    return f"{exchange}/{asset}/{day_iso}/1min.json"

# ----- local file helpers -----
def local_hourly_path(base_dir: str | pathlib.Path, exchange: str, asset: str, day_iso: str, hour: int) -> pathlib.Path:
    p = pathlib.Path(base_dir) / exchange / f"{asset}_{day_iso}_H{hour:02d}_{exchange}.csv"
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

# ----- compose daily from 24 hourly shards (idempotent) -----
def compose_daily_csv(bucket, exchange: str, asset: str, day_iso: str) -> bool:
    prefix = f"{exchange}/{asset}/{day_iso}/"
    parts = []
    for h in range(24):
        k = prefix + f"seconds_H{h:02d}.csv"
        b = bucket.blob(k)
        if b.exists():
            parts.append(b)
    if not parts:
        return False
    out = bucket.blob(prefix + "daily.csv")
    out.compose(parts)     # 24 <= 32-part limit
    out.cache_control = "no-cache, max-age=0"
    out.patch()
    return True
