import requests
from datetime import datetime, timedelta
from pymongo import MongoClient


def fetch_candlesticks(series_ticker, market_ticker, market_open_time, market_close_time, period_interval=1440, extend_days_before=30, extend_days_after=30):
	"""Fetch candlestick data for a market. period_interval: 1 (1min), 60 (1hr), 1440 (1day)
	Extends time range before open_time and after close_time to get more data."""
	try:
		open_dt = datetime.fromisoformat(market_open_time.replace("Z", "+00:00"))
		close_dt = datetime.fromisoformat(market_close_time.replace("Z", "+00:00"))
		
		# Extend time range: go back extend_days_before days before open, and extend_days_after days after close
		start_dt = open_dt - timedelta(days=extend_days_before)
		end_dt = close_dt + timedelta(days=extend_days_after)
		
		# Also extend to current time if close_time is in the past
		now = datetime.now(start_dt.tzinfo)
		if end_dt < now:
			end_dt = now
		
		start_ts = int(start_dt.timestamp())
		end_ts = int(end_dt.timestamp())
		
		url = f"https://api.elections.kalshi.com/trade-api/v2/series/{series_ticker}/markets/{market_ticker}/candlesticks"
		params = {
			"start_ts": start_ts,
			"end_ts": end_ts,
			"period_interval": period_interval
		}
		
		resp = requests.get(url, params=params, timeout=20)
		if resp.status_code != 200:
			return []
		data = resp.json()
		return data.get("candlesticks", [])
	except Exception as e:
		return []


def get_series_ticker_for_event(event_ticker):
	"""Try to get series_ticker for a specific event_ticker"""
	# Try fetching event by event_ticker parameter
	events_url = "https://api.elections.kalshi.com/trade-api/v2/events"
	params = {"event_ticker": event_ticker, "limit": 1}
	
	try:
		resp = requests.get(events_url, params=params, timeout=20)
		if resp.ok:
			data = resp.json()
			events = data.get("events", [])
			if events:
				series_ticker = events[0].get("series_ticker")
				if series_ticker:
					return series_ticker
	except Exception:
		pass
	
	# Fallback: try using event_ticker as series_ticker (sometimes they're the same)
	return None


def main():
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	
	# Read all markets from step_2
	step_2_col = db["step_2"]
	markets = list(step_2_col.find())
	print(f"Found {len(markets)} markets in step_2")
	
	if not markets:
		print("No markets found in step_2. Exiting.")
		return
	
	# Store candlesticks in step_3
	step_3_col = db["step_3"]
	
	success_count = 0
	skip_count = 0
	error_count = 0
	event_to_series_cache = {}  # Cache to avoid repeated API calls
	
	for i, market in enumerate(markets, 1):
		ticker = market.get("ticker")
		event_ticker = market.get("event_ticker")
		open_time = market.get("open_time")
		close_time = market.get("close_time")
		
		if not all([ticker, event_ticker, open_time, close_time]):
			print(f"[{i}/{len(markets)}] Skipping {ticker}: missing required fields")
			skip_count += 1
			continue
		
		# Try to get series_ticker from cache, market, or API
		series_ticker = market.get("series_ticker")
		if not series_ticker:
			series_ticker = event_to_series_cache.get(event_ticker)
			if not series_ticker:
				series_ticker = get_series_ticker_for_event(event_ticker)
				if series_ticker:
					event_to_series_cache[event_ticker] = series_ticker
		
		# Try variations: event_ticker as series_ticker, or try fetching candlesticks directly
		series_tickers_to_try = []
		if series_ticker:
			series_tickers_to_try.append(series_ticker)
		series_tickers_to_try.append(event_ticker)  # Try event_ticker as series_ticker
		
		candlesticks = None
		successful_series = None
		
		for candidate_series in series_tickers_to_try:
			try:
				print(f"[{i}/{len(markets)}] Trying series_ticker '{candidate_series}' for {ticker}...")
				
				# Try multiple period intervals and combine results (daily first, then hourly if needed)
				all_candlesticks = []
				period_intervals = [1440, 60]  # Daily (1440 min) and hourly (60 min)
				
				for period_interval in period_intervals:
					candles = fetch_candlesticks(
						candidate_series, 
						ticker, 
						open_time, 
						close_time, 
						period_interval=period_interval,
						extend_days_before=60,  # Extend 60 days before
						extend_days_after=60    # Extend 60 days after
					)
					if candles:
						all_candlesticks.extend(candles)
				
				if all_candlesticks:
					# Remove duplicates based on end_period_ts
					seen_ts = set()
					unique_candlesticks = []
					for candle in all_candlesticks:
						ts = candle.get("end_period_ts")
						if ts and ts not in seen_ts:
							seen_ts.add(ts)
							unique_candlesticks.append(candle)
					
					# Sort by end_period_ts
					unique_candlesticks.sort(key=lambda x: x.get("end_period_ts", 0))
					
					candlesticks = unique_candlesticks
					successful_series = candidate_series
					if candidate_series != event_ticker:
						event_to_series_cache[event_ticker] = candidate_series
					break
			except Exception as e:
				print(f"  Error processing {candidate_series}: {e}")
				continue
		
		if candlesticks:
			try:
				# Store market with candlesticks in step_3
				market_with_candlesticks = market.copy()
				market_with_candlesticks["candlesticks"] = candlesticks
				step_3_col.replace_one({"ticker": ticker}, market_with_candlesticks, upsert=True)
				print(f"  ✓ Stored {len(candlesticks)} candlesticks")
				success_count += 1
			except Exception as e:
				print(f"  ✗ Error storing to MongoDB: {e}")
				error_count += 1
		else:
			print(f"  ✗ No candlesticks returned (tried {len(series_tickers_to_try)} series_ticker(s))")
			error_count += 1
	
	print(f"\nCompleted:")
	print(f"  Success: {success_count}")
	print(f"  Skipped: {skip_count}")
	print(f"  Errors/No data: {error_count}")
	print(f"  Total: {len(markets)}")


if __name__ == "__main__":
	main()

