import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
import matplotlib.pyplot as plt


def fetch_candlesticks_range(series_ticker, market_ticker, start_ts, end_ts):
	url = (
		f"https://api.elections.kalshi.com/trade-api/v2/series/{series_ticker}/markets/{market_ticker}/candlesticks"
	)
	params = {"start_ts": start_ts, "end_ts": end_ts, "period_interval": 1440}
	resp = requests.get(url, params=params)
	if resp.status_code != 200:
		return []
	return resp.json().get("candlesticks", [])


def main():
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	col = db["kalshi_markets"]

	# Find a market with duration > 6 months
	six_months = timedelta(days=180)
	candidate = None
	for m in col.find({"open_time": {"$exists": True}, "close_time": {"$exists": True}}):
		try:
			open_dt = datetime.fromisoformat(m["open_time"].replace("Z", "+00:00"))
			close_dt = datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
			if close_dt - open_dt >= six_months:
				candidate = m
				break
		except Exception:
			continue

	if not candidate:
		print("No market found with duration >= 6 months.")
		return

	series = candidate.get("series_ticker")
	if not series:
		# Attempt to resolve via events endpoint
		et = candidate.get("event_ticker")
		if et:
			resp = requests.get("https://api.elections.kalshi.com/trade-api/v2/events", params={"limit": 1, "ticker": et})
			if resp.status_code == 200:
				items = resp.json().get("events", [])
				if items:
					series = items[0].get("series_ticker")

	if not series:
		print("Could not resolve series_ticker for candidate market.")
		return

	market_ticker = candidate.get("ticker")
	open_time = candidate.get("open_time")
	close_time = candidate.get("close_time")
	# Fetch full-life daily candles, then take the last 100 with volume > 0
	open_dt = datetime.fromisoformat(open_time.replace("Z", "+00:00"))
	close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
	candles_all = fetch_candlesticks_range(
		series,
		market_ticker,
		int(open_dt.timestamp()),
		int(close_dt.timestamp()),
	) or []

	active_candles = [c for c in candles_all if (c.get("volume") or 0) > 0]
	last_100_active = active_candles[-100:]

	if not last_100_active:
		print("No data available for last 100 ACTIVE days.")
		return

	# Build series (prefer 'close' then 'close_price'); timestamps prefer 'start_ts' then 'ts'
	times = [datetime.utcfromtimestamp(c.get("start_ts") or c.get("ts")) for c in last_100_active]
	closes_raw = [c.get("close") if c.get("close") is not None else c.get("close_price") for c in last_100_active]

	# If prices are in cents, convert to dollars when values look like 0-100 range in cents
	closes = []
	for v in closes_raw:
		if v is None:
			closes.append(None)
			continue
		# Heuristic: if v > 1 and v <= 100, likely cents; convert to dollars
		closes.append(v / 100.0 if 1 < v <= 100 else v)

	# Filter out None
	series_data = [(t, p) for t, p in zip(times, closes) if p is not None]
	if not series_data:
		print("No close prices available to plot.")
		return

	x, y = zip(*series_data)
	plt.figure(figsize=(10, 4))
	plt.plot(x, y, linewidth=1.5)
	plt.title(f"{market_ticker} - Last 100 Active Days (Close)")
	plt.xlabel("Date (UTC)")
	plt.ylabel("Price")
	plt.grid(True, alpha=0.3)
	plt.tight_layout()
	out_path = f"market_{market_ticker}_last_100_active.png"
	plt.savefig(out_path, dpi=150)
	plt.close()
	print(f"Saved: {out_path}")


if __name__ == "__main__":
	main()


