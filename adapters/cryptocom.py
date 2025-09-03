import httpx

# Crypto.com uses BASE_QUOTE with underscore and mostly USDT quote on spot.
BASE_MAP = {
    "BTC": "BTC",
    "ETH": "ETH",
    "ADA": "ADA",
    "SOL": "SOL",
    "XRP": "XRP",
    "LTC": "LTC",
    "BNB": "BNB",
    "XLM": "XLM",
    "LINK": "LINK",
}

async def fetch_orderbook(client: httpx.AsyncClient, base: str, quote: str):
    inst = f"{BASE_MAP.get(base, base)}_{quote}".upper()  # e.g., BTC_USDT
    url = "https://api.crypto.com/v2/public/get-book"
    r = await client.get(url, params={"instrument_name": inst, "depth": 2000}, timeout=5.0)
    r.raise_for_status()
    j = r.json()
    arr = j.get("result", {}).get("data", [])
    if not arr:
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}
    b = arr[0]
    bids = [(float(p), float(q)) for p, q in b.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in b.get("asks", [])]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    price = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else None
    return {"price": price, "best_bid": best_bid, "best_ask": best_ask, "bids": bids, "asks": asks}
