import httpx

# Kraken uses XBT for BTC; many others are the same ticker.
# This map is conservative; extend as you observe more pairs available.
BASE_MAP = {
    "BTC": "XBT",
    "ETH": "ETH",
    "ADA": "ADA",
    "SOL": "SOL",
    "XRP": "XRP",
    "LTC": "LTC",
    "BNB": "BNB",   # Note: BNB is typically NOT listed on Kraken; may return empty.
    "XLM": "XLM",
    "LINK": "LINK",
}

async def fetch_orderbook(client: httpx.AsyncClient, base: str, quote: str):
    pair = f"{BASE_MAP.get(base, base)}{quote}".upper()  # e.g., XBTUSD
    url = "https://api.kraken.com/0/public/Depth"
    r = await client.get(url, params={"pair": pair, "count": 5000}, timeout=5.0)
    r.raise_for_status()
    data = r.json()
    result = data.get("result", {})
    if not result:
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}
    book = next(iter(result.values()))
    bids = [(float(p), float(q)) for p, q, *_ in book.get("bids", [])]
    asks = [(float(p), float(q)) for p, q, *_ in book.get("asks", [])]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    price = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else None
    return {"price": price, "best_bid": best_bid, "best_ask": best_ask, "bids": bids, "asks": asks}
