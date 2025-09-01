import httpx

BASE = "https://api.binance.us"

async def fetch_orderbook(client: httpx.AsyncClient, base: str, quote: str):
    """
    Returns a dict:
    {
      "price": mid,
      "best_bid": float,
      "best_ask": float,
      "bids": [(price, size), ...],  # descending
      "asks": [(price, size), ...],  # ascending
    }
    """
    symbol = f"{base}{quote}"
    url = f"{BASE}/api/v3/depth"
    # Try to fetch deep order book for 5000 levels, fall back to smaller limits if needed
    last_exc = None
    data = None
    for lim in (5000, 1000, 500, 100):
        try:
            params = {"symbol": symbol.upper(), "limit": lim}
            r = await client.get(url, params=params, timeout=5.0)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            last_exc = e
            continue
    if data is None:
        # Re-raise the last exception if all attempts failed
        raise last_exc

    bids = [(float(p), float(s)) for p, s in data.get("bids", [])]
    asks = [(float(p), float(s)) for p, s in data.get("asks", [])]

    # Ensure sorted
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    price = (best_bid + best_ask) / 2.0 if best_bid and best_ask else None

    return {
        "price": price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bids": bids,
        "asks": asks,
    }
