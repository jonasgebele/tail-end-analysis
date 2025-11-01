import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
import time


def fetch_candlesticks_chunked(series_ticker, market_ticker, start_ts, end_ts, period_interval=1440, chunk_days=30, max_retries=3, retry_delay=1):
	"""Fetch candlestick data in chunks to handle API limits and ensure complete data retrieval.
	
	Args:
		series_ticker: The series ticker identifier
		market_ticker: The market ticker identifier
		start_ts: Start timestamp (Unix seconds)
		end_ts: End timestamp (Unix seconds)
		period_interval: Candle interval in minutes (1, 60, 1440)
		chunk_days: Number of days per chunk (default 30)
		max_retries: Maximum retry attempts for failed requests
		retry_delay: Initial delay between retries (seconds)
	
	Returns:
		List of candlestick dictionaries
	"""
	url = f"https://api.elections.kalshi.com/trade-api/v2/series/{series_ticker}/markets/{market_ticker}/candlesticks"
	all_candlesticks = []
	
	# Calculate chunk size in seconds
	chunk_size_seconds = chunk_days * 24 * 60 * 60
	chunk_start = start_ts
	
	while chunk_start < end_ts:
		chunk_end = min(chunk_start + chunk_size_seconds, end_ts)
		
		params = {
			"start_ts": chunk_start,
			"end_ts": chunk_end,
			"period_interval": period_interval
		}
		
		# Retry logic with exponential backoff
		chunk_candles = []
		for attempt in range(max_retries):
			try:
				resp = requests.get(url, params=params, timeout=20)
				
				# Handle rate limiting (429)
				if resp.status_code == 429:
					retry_after = int(resp.headers.get("Retry-After", retry_delay * (2 ** attempt)))
					print(f"    Rate limited. Waiting {retry_after}s before retry {attempt + 1}/{max_retries}...")
					time.sleep(retry_after)
					continue
				
				# Handle other errors
				if resp.status_code != 200:
					if attempt < max_retries - 1:
						wait_time = retry_delay * (2 ** attempt)
						print(f"    HTTP {resp.status_code}. Retrying in {wait_time}s...")
						time.sleep(wait_time)
						continue
					else:
						print(f"    HTTP {resp.status_code} after {max_retries} attempts. Skipping chunk.")
						break
				
				# Success - parse response
				data = resp.json()
				chunk_candles = data.get("candlesticks", [])
				break
				
			except requests.exceptions.RequestException as e:
				if attempt < max_retries - 1:
					wait_time = retry_delay * (2 ** attempt)
					print(f"    Request error: {e}. Retrying in {wait_time}s...")
					time.sleep(wait_time)
				else:
					print(f"    Failed after {max_retries} attempts: {e}")
					break
			except Exception as e:
				print(f"    Unexpected error: {e}")
				break
		
		if chunk_candles:
			all_candlesticks.extend(chunk_candles)
			print(f"    Fetched {len(chunk_candles)} candles for chunk [{datetime.fromtimestamp(chunk_start)} to {datetime.fromtimestamp(chunk_end)}]")
		
		chunk_start = chunk_end
		
		# Small delay between chunks to be respectful to the API
		time.sleep(0.1)
	
	return all_candlesticks


def fetch_candlesticks(series_ticker, market_ticker, market_open_time, market_close_time, period_interval=1440, extend_days_before=30, extend_days_after=30, chunk_days=30):
	"""Fetch candlestick data for a market. period_interval: 1 (1min), 60 (1hr), 1440 (1day)
	Extends time range before open_time and after close_time to get more data.
	Uses chunking to ensure all available data is retrieved."""
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
		
		# Use chunked fetching to ensure complete data retrieval
		return fetch_candlesticks_chunked(
			series_ticker, 
			market_ticker, 
			start_ts, 
			end_ts, 
			period_interval=period_interval,
			chunk_days=chunk_days
		)
	except Exception as e:
		print(f"    Error in fetch_candlesticks: {e}")
		return []


def get_series_ticker_for_event(event_ticker, max_retries=3, retry_delay=1):
	"""Try to get series_ticker for a specific event_ticker with retry logic"""
	events_url = "https://api.elections.kalshi.com/trade-api/v2/events"
	params = {"event_ticker": event_ticker, "limit": 1}
	
	for attempt in range(max_retries):
		try:
			resp = requests.get(events_url, params=params, timeout=20)
			
			# Handle rate limiting
			if resp.status_code == 429:
				retry_after = int(resp.headers.get("Retry-After", retry_delay * (2 ** attempt)))
				time.sleep(retry_after)
				continue
			
			if resp.ok:
				data = resp.json()
				events = data.get("events", [])
				if events:
					series_ticker = events[0].get("series_ticker")
					if series_ticker:
						return series_ticker
			
			# If not 429 and not ok, retry with backoff
			if attempt < max_retries - 1:
				wait_time = retry_delay * (2 ** attempt)
				time.sleep(wait_time)
		except requests.exceptions.RequestException:
			if attempt < max_retries - 1:
				wait_time = retry_delay * (2 ** attempt)
				time.sleep(wait_time)
			else:
				break
		except Exception:
			break
	
	# Fallback: return None (will try event_ticker as series_ticker)
	return None


def main():
	# Configuration
	EXTEND_DAYS_BEFORE = 60  # Days to extend before market open
	EXTEND_DAYS_AFTER = 60   # Days to extend after market close
	CHUNK_DAYS = 30          # Days per chunk for pagination
	# Period intervals to fetch: 1440 (1-day), 60 (1-hour), 1 (1-minute)
	# Note: 1-minute data can be very large - enable only if needed
	PERIOD_INTERVALS = [1440]  # Daily only
	
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	
	# Read all markets from step_2
	step_2_col = db["step_22"]
	markets = list(step_2_col.find())
	print(f"Found {len(markets)} markets in step_2")
	print(f"Configuration:")
	print(f"  Time extension: ±{EXTEND_DAYS_BEFORE} days")
	print(f"  Chunk size: {CHUNK_DAYS} days")
	print(f"  Period intervals: {PERIOD_INTERVALS} minutes ({', '.join(['1-day' if x==1440 else '1-hour' if x==60 else f'{x}-min' for x in PERIOD_INTERVALS])})")
	print()
	
	if not markets:
		print("No markets found in step_2. Exiting.")
		return
	
	# Store candlesticks in step_3
	step_3_col = db["step_33"]
	
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
				
				# Fetch multiple period intervals and combine results
				all_candlesticks = []
				
				for period_interval in PERIOD_INTERVALS:
					interval_name = "1-day" if period_interval == 1440 else "1-hour" if period_interval == 60 else f"{period_interval}-min"
					print(f"  Fetching {interval_name} candles...")
					
					candles = fetch_candlesticks(
						candidate_series, 
						ticker, 
						open_time, 
						close_time, 
						period_interval=period_interval,
						extend_days_before=EXTEND_DAYS_BEFORE,
						extend_days_after=EXTEND_DAYS_AFTER,
						chunk_days=CHUNK_DAYS
					)
					if candles:
						all_candlesticks.extend(candles)
						print(f"  ✓ {interval_name}: {len(candles)} candles")
				
				if all_candlesticks:
					# Remove duplicates based on end_period_ts (and period_interval to handle overlap)
					seen_keys = set()
					unique_candlesticks = []
					for candle in all_candlesticks:
						ts = candle.get("end_period_ts")
						interval = candle.get("period_interval")
						key = (ts, interval) if ts and interval else ts
						if key and key not in seen_keys:
							seen_keys.add(key)
							unique_candlesticks.append(candle)
					
					# Sort by end_period_ts
					unique_candlesticks.sort(key=lambda x: x.get("end_period_ts", 0))
					
					candlesticks = unique_candlesticks
					successful_series = candidate_series
					if candidate_series != event_ticker:
						event_to_series_cache[event_ticker] = candidate_series
					break
			except Exception as e:
				print(f"  ✗ Error processing {candidate_series}: {e}")
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
		
		print()  # Blank line between markets
	
	print(f"\n{'='*60}")
	print(f"Completed:")
	print(f"  Success: {success_count}")
	print(f"  Skipped: {skip_count}")
	print(f"  Errors/No data: {error_count}")
	print(f"  Total: {len(markets)}")
	print(f"{'='*60}")


if __name__ == "__main__":
	main()

