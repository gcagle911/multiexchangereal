import os, csv, time, json, asyncio, pathlib, datetime as dt
import httpx, yaml, importlib
from utils.agg import MinuteAverager
from utils import gcs as GCS

# ---------- math helpers ----------
def iso_now_utc_z():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")

def layered_avg_spread(bids, asks, depth):
    # bids/asks: [(price, size), ...]
    d = min(depth, len(bids), len(asks))
    if d == 0:
        return None
    return sum(asks[i][0] - bids[i][0] for i in range(d)) / d

def pct_of_mid(x, mid):
    if x is None or mid is None or mid <= 0:
        return None
    return (x / mid) * 100.0  # percent of mid

def sum_depth_sizes(rows, depth):
    d = min(depth, len(rows))
    return sum(rows[i][1] for i in range(d)) if d > 0 else 0.0

# ---------- adapter loader ----------
def load_adapter(ex_name: str):
    mod = importlib.import_module(f"adapters.{ex_name}")
    fn = getattr(mod, "fetch_orderbook", None)
    if not fn:
        raise RuntimeError(f"adapters/{ex_name}.py missing fetch_orderbook()")
    return fn

# ---------- main ----------
async def main():
    cfg = yaml.safe_load(pathlib.Path("config.yaml").read_text())
    bucket_name = cfg["gcs_bucket"]
    assets_global = [a.upper() for a in cfg["assets"]]
    row_interval = float(cfg.get("row_interval_seconds", 1))
    upload_interval = int(cfg.get("upload_interval_seconds", 60))

    # Build enabled exchange set using env filters
    all_ex_cfg = cfg["exchanges"]
    include = {s.strip().lower() for s in (os.getenv("EXCHANGES", "")).split(",") if s.strip()}
    exclude = {s.strip().lower() for s in (os.getenv("EXCLUDE_EXCHANGES", "")).split(",") if s.strip()}

    enabled = {}
    for name, ex_cfg in all_ex_cfg.items():
        lname = name.lower()
        if not ex_cfg.get("enabled", False):
            continue
        if include and lname not in include:
            continue
        if lname in exclude:
            continue
        enabled[lname] = {
            "quote": ex_cfg["quote"],
            "fetch": load_adapter(lname),
            # allow per-exchange symbol overrides; else fall back to global assets
            "symbols": [s.upper() for s in ex_cfg.get("symbols", assets_global)],
        }

    if not enabled:
        raise RuntimeError("No exchanges selected (check EXCHANGES/EXCLUDE_EXCHANGES or config.yaml)")

    # GCS
    storage = GCS.storage_client()
    bucket = storage.bucket(bucket_name)

    # HTTP client
    limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
    timeout = httpx.Timeout(5.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        base_dir = pathlib.Path("data")
        base_dir.mkdir(parents=True, exist_ok=True)

        minute = MinuteAverager()
        last_upload = 0.0
        current_day = None

        # Per-exchange local CSV paths for the current UTC day:
        # local_paths[exchange][asset] -> Path
        local_paths = {}

        def init_day(day_iso: str):
            """(Re)initialize per-day local files and minute series for all enabled exchanges."""
            nonlocal local_paths
            local_paths = {}
            for ex_name, meta in enabled.items():
                ex_dir = base_dir / ex_name
                ex_dir.mkdir(parents=True, exist_ok=True)
                local_paths[ex_name] = {}

                for asset in meta["symbols"]:
                    # local daily csv path (per exchange)
                    lp = GCS.local_daily_csv_path(ex_dir, ex_name, asset, day_iso)
                    if not lp.exists():
                        key_csv = GCS.daily_csv_key(ex_name, asset, day_iso)
                        if GCS.blob_exists(bucket, key_csv):
                            GCS.download_blob_to_file(bucket, key_csv, lp)
                        else:
                            GCS.ensure_csv_header(lp)
                    local_paths[ex_name][asset] = lp

                    # resume today's JSON (minute series) if exists
                    key_json = GCS.daily_json_key(ex_name, asset, day_iso)
                    existing = GCS.load_json_if_exists(bucket, key_json) or []
                    minute.replace_series(f"{ex_name}:{asset}", existing)

        while True:
            now_iso = iso_now_utc_z()
            now_dt = dt.datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
            day_iso = now_dt.date().isoformat()

            if current_day != day_iso:
                current_day = day_iso
                init_day(day_iso)

            tick = time.time()

            # fetch all (exchange, asset) concurrently
            async def fetch_one(ex_name, fetch_fn, asset, quote):
                try:
                    ob = await fetch_fn(client, asset, quote)
                    return ex_name, asset, ob, None
                except Exception as e:
                    return ex_name, asset, None, str(e)

            tasks = []
            for ex_name, meta in enabled.items():
                q = meta["quote"]
                fn = meta["fetch"]
                for asset in meta["symbols"]:
                    tasks.append(fetch_one(ex_name, fn, asset, q))

            results = await asyncio.gather(*tasks)

            # process results
            for ex_name, asset, ob, err in results:
                if err or not ob:
                    continue
                # >>> IMPORTANT: skip empty books to avoid blank rows
                if not ob.get("bids") or not ob.get("asks"):
                    continue

                price    = ob["price"]
                best_bid = ob["best_bid"]
                best_ask = ob["best_ask"]

                raw   = (best_ask - best_bid) if (best_ask is not None and best_bid is not None) else None
                L5    = layered_avg_spread(ob["bids"], ob["asks"], 5)
                L20   = layered_avg_spread(ob["bids"], ob["asks"], 20)
                L50   = layered_avg_spread(ob["bids"], ob["asks"], 50)
                L100  = layered_avg_spread(ob["bids"], ob["asks"], 100)
                L5000 = layered_avg_spread(ob["bids"], ob["asks"], 5000)

                s5    = pct_of_mid(L5, price)
                s20   = pct_of_mid(L20, price)
                s50   = pct_of_mid(L50, price)
                s100  = pct_of_mid(L100, price)
                s5000 = pct_of_mid(L5000, price)

                bidv50 = sum_depth_sizes(ob["bids"], 50)
                askv50 = sum_depth_sizes(ob["asks"], 50)

                row = [
                    now_iso, ex_name, asset,
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
                lp = local_paths[ex_name][asset]
                with lp.open("a", newline="") as f:
                    csv.writer(f).writerow(row)

                minute.add(ex_name, asset, now_iso, price, raw, s5, s20, s50, s100, s5000, bidv50, askv50)

            # periodic uploads (per exchange, per asset)
            if time.time() - last_upload >= upload_interval:
                for ex_name in enabled.keys():
                    for asset, lp in local_paths[ex_name].items():
                        key_csv  = GCS.daily_csv_key(ex_name, asset, day_iso)
                        GCS.upload_file(bucket, key_csv, lp, content_type="text/csv")
                        key_json = GCS.daily_json_key(ex_name, asset, day_iso)
                        rows     = minute.series.get(f"{ex_name}:{asset}", [])
                        GCS.upload_json(bucket, key_json, rows)
                last_upload = time.time()

            elapsed = time.time() - tick
            await asyncio.sleep(max(0.0, row_interval - elapsed))

if __name__ == "__main__":
    asyncio.run(main())

