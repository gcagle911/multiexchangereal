import httpx

BASE = "https://api.exchange.coinbase.com"  # public order book (aggregated)

async def fetch_orderbook(client: httpx.AsyncClient, base: str, quote: str):
    """
    Returns a dict:
    {
      "price": mid,
      "best_bid": float,
      "best_ask": float,
      "bids": [(price, size), ...],  # descending by price
      "asks": [(price, size), ...],  # ascending by price
    }
    """
    product = f"{base}-{quote}"
    url = f"{BASE}/products/{product}/book"
    params = {"level": 2}  # aggregated book with many levels
    r = await client.get(url, params=params, timeout=5.0)
    r.raise_for_status()
    data = r.json()

    # Each row format: [price, size, num-orders]
    bids = [(float(row[0]), float(row[1])) for row in data.get("bids", [])]
    asks = [(float(row[0]), float(row[1])) for row in data.get("asks", [])]

    # ensure sorted by price
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    price = (best_bid + best_ask) / 2.0 if (best_bid is not None and best_ask is not None) else None

    return {
        "price": price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bids": bids,
        "asks": asks,
    }

