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
    url = "https://api.bybit.com/spot/v3/public/quote/depth"
    # Bybit spot depth limit typically up to 200
    r = await client.get(url, params={"symbol": symbol, "limit": 200}, timeout=5.0)
    r.raise_for_status()
    d = r.json().get("result", {})
    bids = [(float(p), float(q)) for p, q in d.get("b", [])]
    asks = [(float(p), float(q)) for p, q in d.get("a", [])]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    price = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else None
    return {"price": price, "best_bid": best_bid, "best_ask": best_ask, "bids": bids, "asks": asks}
