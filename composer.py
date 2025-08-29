import os, time, datetime as dt
from utils import gcs as GCS
import yaml, pathlib

def compose_day(bucket, exchanges, assets, day_iso):
    for ex in exchanges:
        for asset in assets:
            try:
                ok = GCS.compose_daily_csv(bucket, ex, asset, day_iso)
                print(f"[composer] {day_iso} {ex}/{asset} -> {'OK' if ok else 'no parts'}")
            except Exception as e:
                print(f"[composer] ERROR {day_iso} {ex}/{asset}: {e}")

def run_loop():
    # read config to know bucket, exchanges, assets
    cfg = yaml.safe_load(pathlib.Path("config.yaml").read_text())
    bucket_name = cfg["gcs_bucket"]
    exchanges = [k for k,v in cfg["exchanges"].items() if v.get("enabled", False)]
    assets = [a.upper() for a in cfg["assets"]]

    client = GCS.storage_client()
    bucket = client.bucket(bucket_name)

    last_day_done = None
    while True:
        now = dt.datetime.utcnow()
        # compose "yesterday" at ~00:03 UTC
        if now.hour == 0 and now.minute >= 3:
            y_iso = (now.date() - dt.timedelta(days=1)).isoformat()
            if y_iso != last_day_done:
                compose_day(bucket, exchanges, assets, y_iso)
                last_day_done = y_iso
        time.sleep(120)  # wake every 2 minutes

if __name__ == "__main__":
    run_loop()
