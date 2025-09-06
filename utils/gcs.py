# utils/gcs.py
from __future__ import annotations

import csv
import io
import json
import pathlib
import time
from typing import List, Optional

import pandas as pd
from google.cloud import storage


# ----- client ---------------------------------------------------------------

def storage_client() -> storage.Client:
    return storage.Client()


# ----- DAILY keys (flat filenames per asset) --------------------------------

def daily_csv_key(exchange: str, asset: str, day_iso: str) -> str:
    # coinbase/ADA/ADA-2025-08-28.csv
    return f"{exchange}/{asset}/{asset}-{day_iso}.csv"

def daily_json_key(exchange: str, asset: str, day_iso: str) -> str:
    # coinbase/ADA/ADA-2025-08-28.json
    return f"{exchange}/{asset}/{asset}-{day_iso}.json"


# ----- local file helpers (unchanged) ---------------------------------------

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
                "spread_L5_pct","spread_L20_pct","spread_L50_pct","spread_L100_pct","spread_L5000_pct",
                "bid_volume_L50","ask_volume_L50"
            ])


# ----- bucket ops (beefed up) ----------------------------------------------

def blob_exists(bucket, key: str) -> bool:
    # Older lib versions require a client on exists(); this keeps it robust.
    b = bucket.blob(key)
    try:
        return b.exists(storage_client())
    except TypeError:
        return b.exists()

def download_blob_to_file(bucket, key: str, local_path: pathlib.Path):
    b = bucket.blob(key)
    data = b.download_as_bytes()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with local_path.open("wb") as f:
        f.write(data)

def _atomic_upload(
    bucket: storage.Bucket,
    key: str,
    data: bytes,
    *,
    content_type: str,
    content_disposition: Optional[str] = None,
    cache_control: str = "public, max-age=60",
    make_public: bool = True,
):
    """Upload to a temp name, copy to final, then delete temp (prevents half files)."""
    tmp_key = f"{key}.tmp.{int(time.time()*1000)}"
    tmp_blob = bucket.blob(tmp_key)
    tmp_blob.cache_control = cache_control
    tmp_blob.content_type = content_type
    if content_disposition:
        tmp_blob.content_disposition = content_disposition
    tmp_blob.upload_from_file(io.BytesIO(data), rewind=True)

    # finalize
    final_blob = bucket.blob(key)
    tmp_blob.copy_to_bucket(bucket, new_name=key)
    tmp_blob.delete()
    if make_public:
        final_blob.make_public()

def upload_file(
    bucket,
    key: str,
    local_path: pathlib.Path,
    content_type: str = "text/csv; charset=utf-8",
    cache_control: str = "public, max-age=60",
    content_disposition: Optional[str] = None,
    make_public: bool = True,
):
    b = bucket.blob(key)
    b.cache_control = cache_control
    b.content_type = content_type
    if content_disposition:
        b.content_disposition = content_disposition
    b.upload_from_filename(str(local_path))
    if make_public:
        b.make_public()

def upload_json(bucket, key: str, obj) -> None:
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    _atomic_upload(
        bucket=bucket,
        key=key,
        data=payload,
        content_type="application/json; charset=utf-8",
        content_disposition=f'attachment; filename="{pathlib.Path(key).name}"',
    )

def load_json_if_exists(bucket, key: str):
    b = bucket.blob(key)
    try:
        if not b.exists(storage_client()):
            return None
    except TypeError:
        if not b.exists():
            return None
    data = b.download_as_bytes()
    return json.loads(data.decode("utf-8"))


# ----- daily composition / publication -------------------------------------

def _shard_keys(exchange: str, asset: str, day_iso: str) -> List[str]:
    # Look for canonical 00/08/16 shards first; if missing, list any prefix match.
    base = f"{exchange}/{asset}/{day_iso}_"
    explicit = [f"{base}{hh}.csv" for hh in ("00", "08", "16")]
    return explicit

def _list_existing_shards(bucket: storage.Bucket, exchange: str, asset: str, day_iso: str) -> List[str]:
    existing = [k for k in _shard_keys(exchange, asset, day_iso) if blob_exists(bucket, k)]
    if existing:
        return existing
    # fallback: any CSV with the prefix
    prefix = f"{exchange}/{asset}/{day_iso}_"
    blobs = list(bucket.list_blobs(prefix=prefix))
    return [b.name for b in blobs if b.name.endswith(".csv")]

def _download_csv(bucket: storage.Bucket, key: str) -> pd.DataFrame:
    raw = bucket.blob(key).download_as_bytes()
    return pd.read_csv(io.BytesIO(raw))

def _normalize_time(df: pd.DataFrame) -> pd.DataFrame:
    tcol = "time" if "time" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if tcol is None:
        raise ValueError("No 'time' or 'timestamp' column found")
    df["time"] = pd.to_datetime(df[tcol], utc=True, errors="coerce")
    return df.dropna(subset=["time"]).sort_values("time")

def _daily_json_bytes(day_df: pd.DataFrame) -> bytes:
    preferred = [
        "time","exchange","asset","price","price_avg","bid","ask","spread","volume",
        "spread_avg_L20","spread_avg_L20_pct",
        "spread_L5_pct","spread_L20_pct","spread_L50_pct","spread_L100_pct",
        "bid_volume_L50","ask_volume_L50",
    ]
    cols = [c for c in preferred if c in day_df.columns]
    jdf = day_df[cols].copy() if cols else day_df.copy()
    jdf = jdf.replace([float("inf"), float("-inf")], pd.NA).where(lambda x: x.notna(), None)
    return json.dumps(jdf.to_dict(orient="records"), ensure_ascii=False).encode("utf-8")

def compose_daily_csv(bucket: storage.Bucket, exchange: str, asset: str, day_iso: str) -> bool:
    """
    Publish a clean daily CSV + JSON at:
      {ex}/{asset}/{ASSET-YYYY-MM-DD}.csv
      {ex}/{asset}/{ASSET-YYYY-MM-DD}.json

    Works if you have:
      (a) shards   -> ex/asset/YYYY-MM-DD_00.csv, _08.csv, _16.csv
      (b) a single -> ex/asset/ASSET-YYYY-MM-DD.csv
    """
    # Case (a): shards present
    shards = _list_existing_shards(bucket, exchange, asset, day_iso)
    df_day = None
    if shards:
        dfs = []
        for k in shards:
            try:
                dfs.append(_download_csv(bucket, k))
            except Exception as e:
                print(f"[compose] WARN read {k} failed: {e}")
        if dfs:
            df_day = pd.concat(dfs, ignore_index=True)
            df_day = _normalize_time(df_day)

    # Case (b): single CSV already there -> still ensure proper headers/public & produce JSON
    single_key = daily_csv_key(exchange, asset, day_iso)
    if df_day is None and blob_exists(bucket, single_key):
        # Re-read and re-publish JSON with good headers
        df_day = _download_csv(bucket, single_key)
        df_day = _normalize_time(df_day)
        # also re-upload CSV with correct content-disposition so phones open it nicely
        csv_bytes = df_day.to_csv(index=False, lineterminator="\n").encode("utf-8")
        _atomic_upload(
            bucket=bucket,
            key=single_key,
            data=csv_bytes,
            content_type="text/csv; charset=utf-8",
            content_disposition=f'attachment; filename="{asset}-{day_iso}.csv"',
        )
        print(f"[compose] (fix headers) CSV → https://storage.googleapis.com/{bucket.name}/{single_key}")

    if df_day is None:
        # Nothing to publish
        return False

    # Publish CSV
    csv_key = single_key
    csv_bytes = df_day.to_csv(index=False, lineterminator="\n").encode("utf-8")
    _atomic_upload(
        bucket=bucket,
        key=csv_key,
        data=csv_bytes,
        content_type="text/csv; charset=utf-8",
        content_disposition=f'attachment; filename="{asset}-{day_iso}.csv"',
    )
    print(f"[compose] CSV  → https://storage.googleapis.com/{bucket.name}/{csv_key}")

    # Publish JSON (skip if 0 rows)
    if len(df_day) > 0:
        json_key = daily_json_key(exchange, asset, day_iso)
        _atomic_upload(
            bucket=bucket,
            key=json_key,
            data=_daily_json_bytes(df_day),
            content_type="application/json; charset=utf-8",
            content_disposition=f'attachment; filename="{asset}-{day_iso}.json"',
        )
        print(f"[compose] JSON → https://storage.googleapis.com/{bucket.name}/{json_key}")
    else:
        print(f"[compose] JSON skipped (0 rows) for {exchange}/{asset} {day_iso}")

    return True
