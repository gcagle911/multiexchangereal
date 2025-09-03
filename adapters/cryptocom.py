import httpx

# Crypto.com uses BASE_QUOTE with underscore for spot.
# Skip BNB on Crypto.com (not listed), include the rest.
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

async def _get_book(client: httpx.AsyncClient, instrument: str, depth: int = 150):
    url = "https://api.crypto.com/v2/public/get-book"
    r = await client.get(url, params={"instrument_name": instrument, "depth": depth}, timeout=5.0)
    r.raise_for_status()
    j = r.json()
    # Crypto.com success => code == 0 and result.data[0] exists
    if j.get("code", 0) != 0:
        return None
    arr = j.get("result", {}).get("data", [])
    if not arr:
        return None
    return arr[0]

async def fetch_orderbook(client: httpx.AsyncClient, base: str, quote: str):
    # BNB not supported on Crypto.com spot in your set — return empty so logger skips.
    if base.upper() == "BNB":
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}

    base_sym = BASE_MAP.get(base.upper(), base.upper())

    # Try requested quote first (likely USDT), then fallback to USD if empty.
    preferred = f"{base_sym}_{quote}".upper()
    fallback  = f"{base_sym}_USD"

    book = await _get_book(client, preferred, depth=150)
    if book is None:
        book = await _get_book(client, fallback, depth=150)

    if book is None:
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}

    bids = [(float(p), float(q)) for p, q in book.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in book.get("asks", [])]

    # Ensure sorted
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    if not bids or not asks:
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    price = (best_bid + best_ask) / 2.0
    return {"price": price, "best_bid": best_bid, "best_ask": best_ask, "bids": bids, "asks": asks}
