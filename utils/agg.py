import datetime as dt

def minute_bucket(ts_iso: str) -> str:
    # '2025-08-28T14:03:07Z' -> '2025-08-28T14:03:00Z'
    t = dt.datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
    t = t.replace(second=0, microsecond=0, tzinfo=dt.timezone.utc)
    return t.isoformat().replace("+00:00","Z")

class MinuteAverager:
    """
    Keeps per-asset rolling sums for 1-minute snapshots.
    Each minute we store ONE object with averages of the per-second fields.
    Averages stored:
      - price (mid)
      - spread_raw
      - spread_L5_pct, spread_L20_pct, spread_L50_pct, spread_L100_pct
      - bid_volume_L50, ask_volume_L50   (order-book depth, asset units)
    Output entry example (per minute):
      {
        "t": "...Z",
        "exchange": "coinbase",
        "asset": "ADA",
        "price_avg": ...,
        "spread_raw_avg": ...,
        "spread_L5_pct_avg": ...,
        "spread_L20_pct_avg": ...,
        "spread_L50_pct_avg": ...,
        "spread_L100_pct_avg": ...,
        "bid_volume_L50_avg": ...,
        "ask_volume_L50_avg": ...
      }
    """

    def __init__(self):
        self.state = {}   # asset -> current minute accumulators
        self.series = {}  # asset -> list of daily minute entries

    def _start(self, key, exchange, asset, price, spread_raw, s5, s20, s50, s100, bidv50, askv50):
        def v(x): return float(x) if x is not None else None
        return {
            "key": key,
            "exchange": exchange,
            "asset": asset,
            "n": 1,
            "price_sum": v(price) or 0.0,
            "spread_raw_sum": v(spread_raw) or 0.0,
            "s5_sum": v(s5) or 0.0,
            "s20_sum": v(s20) or 0.0,
            "s50_sum": v(s50) or 0.0,
            "s100_sum": v(s100) or 0.0,
            "bidv50_sum": v(bidv50) or 0.0,
            "askv50_sum": v(askv50) or 0.0,
        }

    def _update(self, st, price, spread_raw, s5, s20, s50, s100, bidv50, askv50):
        def add(k, x):
            if x is None: return
            st[k] += float(x)
        st["n"] += 1
        add("price_sum", price)
        add("spread_raw_sum", spread_raw)
        add("s5_sum", s5)
        add("s20_sum", s20)
        add("s50_sum", s50)
        add("s100_sum", s100)
        add("bidv50_sum", bidv50)
        add("askv50_sum", askv50)

    def _finalize(self, st):
        n = max(1, st["n"])
        return {
            "t": st["key"],
            "exchange": st["exchange"],
            "asset": st["asset"],
            "price_avg": st["price_sum"] / n,
            "spread_raw_avg": st["spread_raw_sum"] / n,
            "spread_L5_pct_avg": st["s5_sum"] / n,
            "spread_L20_pct_avg": st["s20_sum"] / n,
            "spread_L50_pct_avg": st["s50_sum"] / n,
            "spread_L100_pct_avg": st["s100_sum"] / n,
            "bid_volume_L50_avg": st["bidv50_sum"] / n,
            "ask_volume_L50_avg": st["askv50_sum"] / n,
        }

    def add(self, exchange, asset, ts_iso, price, spread_raw, s5, s20, s50, s100, bidv50, askv50):
        key = minute_bucket(ts_iso)
        if asset not in self.series:
            self.series[asset] = []
        st = self.state.get(asset)
        if (st is None) or (st["key"] != key):
            # rollover previous minute (if any)
            if st is not None:
                self.series[asset].append(self._finalize(st))
                if len(self.series[asset]) > 1440:
                    self.series[asset] = self.series[asset][-1440:]
            # start new minute
            self.state[asset] = self._start(key, exchange, asset, price, spread_raw, s5, s20, s50, s100, bidv50, askv50)
        else:
            self._update(st, price, spread_raw, s5, s20, s50, s100, bidv50, askv50)

    def replace_series(self, asset, rows):
        self.series[asset] = rows
        self.state[asset] = None
