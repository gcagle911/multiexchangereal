import httpx

# Crypto.com uses BASE_QUOTE with underscore for spot (e.g., BTC_USDT).
# You said CRO has all your assets except BNB; we’ll skip BNB explicitly.
BASE_MAP = {
    "BTC": "BTC",
    "ETH": "ETH",
    "ADA": "ADA",
    "SOL": "SOL",
    "XRP": "XRP",
    "LTC": "LTC",
    "XLM": "XLM",
    "LINK": "LINK",
    # "BNB": "BNB",  # intentionally omitted for Crypto.com
}

EMPTY = {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}

async def _get_book(client: httpx.AsyncClient, instrument: str, depth: int = 150):
    url = "https://api.crypto.com/v2/public/get-book"
    try:
        r = await client.get(url, params={"instrument_name": instrument, "depth": depth}, timeout=5.0)
        r.raise_for_status()
        j = r.json()
        # Success => code == 0 and result.data exists
        if j.get("code", 0) != 0:
            return None
        arr = j.get("result", {}).get("data", [])
        return arr[0] if arr else None
    except Exception:
        return None

async def fetch_orderbook(client: httpx.AsyncClient, base: str, quote: str):
    base_u = base.upper()
    if base_u == "BNB":
        return EMPTY  # skip BNB on Crypto.com per your note

    base_sym = BASE_MAP.get(base_u, base_u)

    # Prefer requested quote (likely USDT). Fallback to USD transparently.
    preferred = f"{base_sym}_{quote}".upper()
    fallback  = f"{base_sym}_USD"

    book = await _get_book(client, preferred, depth=150)
    if not book:
        book = await _get_book(client, fallback, depth=150)

    if not book:
        return EMPTY

    bids = [(float(p), float(q)) for p, q in book.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in book.get("asks", [])]

    # Sort & validate
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    if not bids or not asks:
        return EMPTY

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    price = (best_bid + best_ask) / 2.0
    return {"price": price, "best_bid": best_bid, "best_ask": best_ask, "bids": bids, "asks": asks}
