import os, csv, time, json, asyncio, pathlib, datetime as dt
import httpx, yaml
from utils.agg import MinuteAverager
from utils import gcs as GCS

# ---------- math helpers ----------
def iso_now_utc_z():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z")

def layered_avg_spread(bids, asks, depth):
    # bids/asks: [(price, size), ...]
    d = min(depth, len(bids), len(asks))
    if d == 0: return None
    return sum(asks[i][0] - bids[i][0] for i in range(d)) / d

def pct_of_mid(x, mid):
    if x is None or mid is None or mid <= 0: return None
    return (x / mid) * 100.0

def sum_depth_sizes(rows, depth):
    d = min(depth, len(rows))
    return sum(rows[i][1] for i in range(d)) if d > 0 else 0.0

# ---------- main ----------
async def main():
    exchange = os.environ.get("EXCHANGE", "coinbase").lower()

    cfg = yaml.safe_load(pathlib.Path("config.yaml").read_text())
    bucket_name = cfg["gcs_bucket"]
    assets = [a.upper() for a in cfg["assets"]]
    row_interval = float(cfg.get("row_interval_seconds", 1))
    upload_interval = int(cfg.get("upload_interval_seconds", 60))

    ex_cfg = cfg["exchanges"].get(exchange)
    if not ex_cfg or not ex_cfg.get("enabled", False):
        raise RuntimeError(f"Exchange '{exchange}' not enabled in config.yaml")
    quote = ex_cfg["quote"]

    # adapter switch
    if exchange == "coinbase":
        from adapters.coinbase import fetch_orderbook
    elif exchange == "binanceus":
        from adapters.binanceus import fetch_orderbook
    else:
        raise RuntimeError(f"Unknown EXCHANGE={exchange}")

    # GCS
    storage = GCS.storage_client()
    bucket = storage.bucket(bucket_name)

    # HTTP client
    limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
    async with httpx.AsyncClient(limits=limits, timeout=5.0) as client:
        base_dir = pathlib.Path("data") / exchange
        base_dir.mkdir(parents=True, exist_ok=True)

        minute = MinuteAverager()
        last_upload = 0.0
        current_day = None
        local_paths = {}  # asset -> local csv path for today

        # helper: (re)initialize for a UTC day (create/download daily CSV; load JSON if exists)
        def init_day(day_iso: str):
            nonlocal local_paths
            local_paths = {}
            for asset in assets:
                # local daily csv path
                lp = GCS.local_daily_csv_path(base_dir, exchange, asset, day_iso)
                if not lp.exists():
                    # if a daily file already exists in GCS (restart), download to continue appending
                    key_csv = GCS.daily_csv_key(exchange, asset, day_iso)
                    if GCS.blob_exists(bucket, key_csv):
                        GCS.download_blob_to_file(bucket, key_csv, lp)
                    else:
                        GCS.ensure_csv_header(lp)
                local_paths[asset] = lp

                # resume today's JSON if exists
                key_json = GCS.daily_json_key(exchange, asset, day_iso)
                existing = GCS.load_json_if_exists(bucket, key_json) or []
                # replace series in memory; we don't try to reconstruct a partially open minute (fine for daily snapshot averaging)
                minute.replace_series(asset, existing)

        while True:
            now_iso = iso_now_utc_z()
            now_dt = dt.datetime.fromisoformat(now_iso.replace("Z","+00:00"))
            day_iso = now_dt.date().isoformat()

            # day rollover handling
            if current_day != day_iso:
                current_day = day_iso
                init_day(day_iso)

            tick = time.time()

            # fetch all assets concurrently
            async def fetch_one(asset):
                try:
                    ob = await fetch_orderbook(client, asset, quote)
                    return asset, ob, None
                except Exception as e:
                    return asset, None, str(e)

            results = await asyncio.gather(*[fetch_one(a) for a in assets])

            # process results
            for asset, ob, err in results:
                if err or not ob:
                    continue

                price = ob["price"]
                best_bid = ob["best_bid"]
                best_ask = ob["best_ask"]

                # spreads
                raw = (best_ask - best_bid) if (best_ask is not None and best_bid is not None) else None
                L5   = layered_avg_spread(ob["bids"], ob["asks"], 5)
                L20  = layered_avg_spread(ob["bids"], ob["asks"], 20)
                L50  = layered_avg_spread(ob["bids"], ob["asks"], 50)
                L100 = layered_avg_spread(ob["bids"], ob["asks"], 100)
                L5000 = layered_avg_spread(ob["bids"], ob["asks"], 5000)

                s5   = pct_of_mid(L5, price)
                s20  = pct_of_mid(L20, price)
                s50  = pct_of_mid(L50, price)
                s100 = pct_of_mid(L100, price)
                s5000 = pct_of_mid(L5000, price)

                # depth "volume" (asset units) from top 50 levels
                bidv50 = sum_depth_sizes(ob["bids"], 50)
                askv50 = sum_depth_sizes(ob["asks"], 50)

                # CSV append (daily file)
                row = [
                    now_iso, exchange, asset,
                    f"{price:.10f}" if price is not None else "",
                    f"{best_bid:.10f}" if best_bid is not None else "",
                    f"{best_ask:.10f}" if best_ask is not None else "",
                    f"{raw:.10f}" if raw is not None else "",
                    f"{s5:.10f}" if s5 is not None else "",
                    f"{s20:.10f}" if s20 is not None else "",
                    f"{s50:.10f}" if s50 is not None else "",
                    f"{s100:.10f}" if s100 is not None else "",
                    f"{s5000:.10f}" if s5000 is not None else "",
                    f"{bidv50:.10f}",
                    f"{askv50:.10f}",
                ]
                with local_paths[asset].open("a", newline="") as f:
                    csv.writer(f).writerow(row)

                # 1‑minute averages (daily JSON content)
                minute.add(exchange, asset, now_iso, price, raw, s5, s20, s50, s100, s5000, bidv50, askv50)

            # periodic uploads: write today's CSV and JSON to final DAILY filenames
            if time.time() - last_upload >= upload_interval:
                for asset, lp in local_paths.items():
                    key_csv = GCS.daily_csv_key(exchange, asset, day_iso)
                    GCS.upload_file(bucket, key_csv, lp, content_type="text/csv")
                    key_json = GCS.daily_json_key(exchange, asset, day_iso)
                    rows = minute.series.get(asset, [])
                    GCS.upload_json(bucket, key_json, rows)
                last_upload = time.time()

            # pacing to target cadence
            elapsed = time.time() - tick
            await asyncio.sleep(max(0.0, row_interval - elapsed))

if __name__ == "__main__":
    asyncio.run(main())
