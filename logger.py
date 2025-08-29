import os, csv, time, json, asyncio, pathlib, datetime as dt
import httpx, yaml
from utils.agg import MinuteAverager
from utils import gcs as GCS

# ---------- math helpers ----------
def iso_now_utc_z():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00","Z")

def layered_avg_spread(bids, asks, depth):
    # bids/asks are lists of (price, size)
    d = min(depth, len(bids), len(asks))
    if d == 0: return None
    return sum(asks[i][0] - bids[i][0] for i in range(d)) / d

def pct_of_mid(x, mid):
    if x is None or mid is None or mid <= 0: return None
    return (x / mid) * 100.0

def sum_depth_sizes(rows, depth):
    # rows are (price, size)
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

        while True:
            tick = time.time()
            now_iso = iso_now_utc_z()
            now_dt = dt.datetime.fromisoformat(now_iso.replace("Z","+00:00"))
            day_iso = now_dt.date().isoformat()
            hour = now_dt.hour

            # ensure local hourly CSV paths exist (resume if shard already in GCS)
            paths = {}
            for asset in assets:
                lp = GCS.local_hourly_path(base_dir, exchange, asset, day_iso, hour)
                if not lp.exists():
                    key = GCS.hourly_csv_key(exchange, asset, day_iso, hour)
                    if GCS.blob_exists(bucket, key):
                        # resume by downloading existing shard
                        GCS.download_blob_to_file(bucket, key, lp)
                    else:
                        GCS.ensure_csv_header(lp)
                paths[asset] = lp

            # ensure today's 1min.json (if any) is loaded into memory (resume)
            for asset in assets:
                json_key = GCS.daily_json_key(exchange, asset, day_iso)
                if asset not in minute.series:
                    existing = GCS.load_json_if_exists(bucket, json_key) or []
                    minute.replace_series(asset, existing)

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
                L5  = layered_avg_spread(ob["bids"], ob["asks"], 5)
                L20 = layered_avg_spread(ob["bids"], ob["asks"], 20)
                L50 = layered_avg_spread(ob["bids"], ob["asks"], 50)
                L100= layered_avg_spread(ob["bids"], ob["asks"], 100)

                s5   = pct_of_mid(L5, price)
                s20  = pct_of_mid(L20, price)
                s50  = pct_of_mid(L50, price)
                s100 = pct_of_mid(L100, price)

                # depth "volume" (asset units) from top 50 levels
                bidv50 = sum_depth_sizes(ob["bids"], 50)
                askv50 = sum_depth_sizes(ob["asks"], 50)

                # CSV
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
                    f"{bidv50:.10f}",
                    f"{askv50:.10f}",
                ]
                with paths[asset].open("a", newline="") as f:
                    csv.writer(f).writerow(row)

                # minute averages JSON
                minute.add(exchange, asset, now_iso, price, raw, s5, s20, s50, s100, bidv50, askv50)

            # periodic upload of current hour CSV + today's 1min.json
            if time.time() - last_upload >= upload_interval:
                for asset in assets:
                    # upload shard
                    key = GCS.hourly_csv_key(exchange, asset, day_iso, hour)
                    GCS.upload_file(bucket, key, paths[asset], content_type="text/csv")
                    # upload daily JSON (static path; rewritten during the day)
                    json_key = GCS.daily_json_key(exchange, asset, day_iso)
                    rows = minute.series.get(asset, [])
                    GCS.upload_json(bucket, json_key, rows)
                last_upload = time.time()

            # pacing to target 1 second cadence
            elapsed = time.time() - tick
            await asyncio.sleep(max(0.0, row_interval - elapsed))

if __name__ == "__main__":
    asyncio.run(main())
