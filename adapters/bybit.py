import httpx

# Bybit spot uses BASEQUOTE like BTCUSDT
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
    symbol = f"{BASE_MAP.get(base, base)}{quote}".upper()  # e.g., BTCUSDT
    # v5 spot orderbook (more reliable globally)
    url = "https://api.bybit.com/v5/market/orderbook"
    params = {"category": "spot", "symbol": symbol, "limit": 200}
    r = await client.get(url, params=params, timeout=5.0)
    r.raise_for_status()
    j = r.json()
    if j.get("retCode") != 0:
        # gracefully return empty
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}

    lst = j.get("result", {}).get("list", [])
    if not lst:
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}

    entry = lst[0]
    # Bybit v5 returns strings under "b" and "a" arrays: [["price","size"], ...]
    bids = [(float(p), float(q)) for p, q in entry.get("b", [])]
    asks = [(float(p), float(q)) for p, q in entry.get("a", [])]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    if not bids or not asks:
        return {"price": None, "best_bid": None, "best_ask": None, "bids": [], "asks": []}

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    price = (best_bid + best_ask) / 2.0
    return {"price": price, "best_bid": best_bid, "best_ask": best_ask, "bids": bids, "asks": asks}
