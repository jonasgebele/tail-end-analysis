import requests
from datetime import datetime
from pymongo import MongoClient


def fetch_markets(max_markets=10000):
	url = "https://api.elections.kalshi.com/trade-api/v2/markets"
	statuses_to_try = ["settled", "closed", "open"]
	all_markets = []

	for status in statuses_to_try:
		params = {
			"limit": 1000,
			"status": status,
		}
		cursor = None
		first_page_reported = False
		
		while True:
			if cursor:
				params["cursor"] = cursor
			else:
				params.pop("cursor", None)
			try:
				resp = requests.get(url, params=params, timeout=20)
				if not resp.ok:
					print(f"Markets API status {resp.status_code} for status='{status}'")
					break
				data = resp.json()
			except Exception as e:
				print(f"Markets API request failed for status='{status}': {e}")
				break
			if "error" in data:
				print(f"Markets API error for status='{status}': {data.get('error')}")
				break
			markets = data.get("markets", [])
			if not first_page_reported:
				print(f"Markets API first page (status='{status}') returned {len(markets)} items")
				first_page_reported = True
			all_markets.extend(markets)
			if len(all_markets) >= max_markets:
				all_markets = all_markets[:max_markets]
				break
			cursor = data.get("cursor")
			if not cursor or not markets:
				break
		if all_markets:
			break

	if not all_markets:
		print("Markets API returned no data across statuses: settled, closed, open")
	return all_markets


def fetch_candlesticks(series_ticker, market_ticker, market_close_time, days=30):
	close_dt = datetime.fromisoformat(market_close_time.replace("Z", "+00:00"))
	end_ts = int(close_dt.timestamp())
	start_ts = end_ts - days * 24 * 60 * 60
	url = (
		f"https://api.elections.kalshi.com/trade-api/v2/series/{series_ticker}/markets/{market_ticker}/candlesticks"
	)
	params = {"start_ts": start_ts, "end_ts": end_ts, "period_interval": 1440}
	resp = requests.get(url, params=params)
	if resp.status_code != 200:
		return []
	return resp.json().get("candlesticks", [])



def _parse_iso_utc(ts_str):
	"""Parse ISO8601 timestamp strings that may end with 'Z' into aware datetimes."""
	if not ts_str:
		return None
	try:
		return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
	except Exception:
		return None


def _is_open_longer_than_two_months(market):
	open_time_str = market.get("open_time")
	close_time_str = market.get("close_time")
	open_dt = _parse_iso_utc(open_time_str)
	close_dt = _parse_iso_utc(close_time_str)
	if not open_dt or not close_dt:
		return False
	# Approximate two months as 60 days using total seconds for accuracy
	duration_seconds = (close_dt - open_dt).total_seconds()
	return duration_seconds >= 60 * 24 * 60 * 60


def _has_last_price_above_point_nine(market):
	# Accept either integer cents (>= 90) or dollar string (>= 0.9)
	last_price = market.get("last_price")
	last_price_dollars = market.get("last_price_dollars")
	try:
		if isinstance(last_price, (int, float)):
			if last_price >= 90:
				return True
	except Exception:
		pass
	try:
		if isinstance(last_price_dollars, str):
			if float(last_price_dollars) >= 0.9:
				return True
	except Exception:
		pass
	return False

def main():
	markets = fetch_markets(max_markets=100000)


	# Apply filters (only duration check):
	filtered = [
		m for m in markets
		if _is_open_longer_than_two_months(m)
	]

	print(
		f"Totals — fetched: {len(markets)}, duration>=60d: "
		f"{sum(1 for m in markets if _is_open_longer_than_two_months(m))}, "
		f"final: {len(filtered)}"
	)

	for i, m in enumerate(filtered, 1):
		series = m.get("series_ticker")
		if series and m.get("ticker") and m.get("close_time"):
			m["candlesticks"] = fetch_candlesticks(series, m["ticker"], m["close_time"]) or []

	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	col = db["kalshi_markets"]
	for m in filtered:
		col.replace_one({"ticker": m.get("ticker")}, m, upsert=True)

	print(f"Stored {len(filtered)} markets (open>=2 months) into MongoDB.")


if __name__ == "__main__":
	main()
