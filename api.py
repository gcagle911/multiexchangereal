from flask_cors import CORS
import os, re
from flask import Flask, Response, jsonify, request, abort
from google.cloud import storage

app = Flask(__name__)
CORS(app)

BUCKET = os.environ.get("GCS_BUCKET", "multicryptoreal")
client = storage.Client()
bucket = client.bucket(BUCKET)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def cors(resp):
    # allow your site(s) to fetch; adjust to your exact domains if you want to restrict
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp

def fetch_bytes_or_404(key: str, mime: str):
    b = bucket.blob(key)
    if not b.exists():
        abort(404, f"not found: {key}")
    data = b.download_as_bytes()
    return cors(Response(data, mimetype=mime))

@app.get("/files/json")
def get_daily_json():
    ex = request.args.get("exchange","").lower()
    asset = request.args.get("asset","").upper()
    day = request.args.get("day","")
    if not ex or not asset or not DATE_RE.match(day):
        abort(400, "params: exchange, asset, day=YYYY-MM-DD required")
    key = f"{ex}/{asset}/{asset}-{day}.json"
    return fetch_bytes_or_404(key, "application/json")

@app.get("/files/csv")
def get_daily_csv():
    ex = request.args.get("exchange","").lower()
    asset = request.args.get("asset","").upper()
    day = request.args.get("day","")
    if not ex or not asset or not DATE_RE.match(day):
        abort(400, "params: exchange, asset, day=YYYY-MM-DD required")
    key = f"{ex}/{asset}/{asset}-{day}.csv"
    return fetch_bytes_or_404(key, "text/csv")

@app.get("/list/days")
def list_days():
    ex = request.args.get("exchange","").lower()
    asset = request.args.get("asset","").upper()
    if not ex or not asset:
        abort(400, "params: exchange, asset required")
    prefix = f"{ex}/{asset}/"
    days = []
    for blob in client.list_blobs(BUCKET, prefix=prefix):
        name = blob.name  # e.g., coinbase/ADA/ADA-2025-08-28.json
        if name.endswith(".json"):
            fname = name.split("/")[-1]          # ADA-2025-08-28.json
            date_part = fname[len(asset)+1:-5]   # 2025-08-28
            if DATE_RE.match(date_part):
                days.append(date_part)
    days = sorted(set(days))
    return cors(Response(response=jsonify({"exchange": ex, "asset": asset, "days": days}).data,
                         mimetype="application/json"))

@app.get("/")
def root():
    return cors(Response('{"ok":true}', mimetype="application/json"))
@app.get("/list/exchanges")
def list_exchanges():
    # exchanges are the first path segment (e.g., coinbase/, binanceus/)
    seen = set()
    for blob in client.list_blobs(BUCKET, delimiter="/"):
        pass
    # The Storage client puts prefixes in _get_next_page_response()['prefixes'], but Flask route can't access it directly.
    # Simpler: list all blobs and extract first segment.
    for blob in client.list_blobs(BUCKET):
        parts = blob.name.split("/", 2)
        if len(parts) >= 1 and parts[0]:
            seen.add(parts[0])
    return cors(Response(response=jsonify({"exchanges": sorted(seen)}).data, mimetype="application/json"))

@app.get("/list/assets")
def list_assets():
    ex = request.args.get("exchange","").lower()
    if not ex:
        abort(400, "param: exchange required")
    assets = set()
    prefix = f"{ex}/"
    for blob in client.list_blobs(BUCKET, prefix=prefix):
        parts = blob.name.split("/")
        if len(parts) >= 2 and parts[1]:
            assets.add(parts[1])
    return cors(Response(response=jsonify({"exchange": ex, "assets": sorted(assets)}).data, mimetype="application/json"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
