from datetime import datetime, timedelta
from pymongo import MongoClient


def get_price_from_candle(candle):
	"""Extract price from candlestick, preferring close over mean"""
	price_obj = candle.get("price", {})
	close_price = price_obj.get("close")
	mean_price = price_obj.get("mean")
	
	if close_price is not None:
		return close_price / 100.0  # Convert cents to dollars
	elif mean_price is not None:
		return mean_price / 100.0  # Convert cents to dollars
	return None


def has_price_above_threshold_for_duration(candlesticks, threshold=0.8, min_duration_days=7):
	"""Check if market has price >= threshold for at least min_duration_days consecutive days"""
	if not candlesticks:
		return False
	
	# Sort candlesticks by timestamp
	sorted_candles = sorted(candlesticks, key=lambda x: x.get("end_period_ts", 0))
	
	# Track consecutive periods above threshold
	current_streak_start = None
	max_streak_days = 0
	
	for candle in sorted_candles:
		end_ts = candle.get("end_period_ts")
		if not end_ts:
			continue
		
		price = get_price_from_candle(candle)
		if price is None:
			continue
		
		dt = datetime.fromtimestamp(end_ts)
		
		if price >= threshold:
			if current_streak_start is None:
				current_streak_start = dt
		else:
			# Price dropped below threshold, check if we had a long enough streak
			if current_streak_start is not None:
				streak_duration = (dt - current_streak_start).days
				max_streak_days = max(max_streak_days, streak_duration)
				if max_streak_days >= min_duration_days:
					return True
			current_streak_start = None
	
	# Check final streak if we ended while still above threshold
	if current_streak_start is not None:
		last_dt = datetime.fromtimestamp(sorted_candles[-1].get("end_period_ts", 0))
		streak_duration = (last_dt - current_streak_start).days
		max_streak_days = max(max_streak_days, streak_duration)
	
	return max_streak_days >= min_duration_days


def main():
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	
	# Read all markets from step_3
	step_3_col = db["step_3"]
	markets = list(step_3_col.find())
	print(f"Found {len(markets)} markets in step_3")
	
	if not markets:
		print("No markets found in step_3. Exiting.")
		return
	
	# Filter markets: price >= 0.8 (80%) for at least 7 consecutive days
	filtered_markets = []
	
	for i, market in enumerate(markets, 1):
		ticker = market.get("ticker", f"market_{i}")
		candlesticks = market.get("candlesticks", [])
		
		if not candlesticks:
			continue
		
		if has_price_above_threshold_for_duration(candlesticks, threshold=0.95, min_duration_days=7):
			filtered_markets.append(market)
			print(f"[{i}/{len(markets)}] ✓ {ticker}: price >= 80% for >= 7 days")
		else:
			print(f"[{i}/{len(markets)}] ✗ {ticker}: does not meet criteria")
	
	# Store filtered markets in step_4
	step_4_col = db["step_4"]
	for market in filtered_markets:
		ticker = market.get("ticker")
		step_4_col.replace_one({"ticker": ticker}, market, upsert=True)
	
	print(f"\nCompleted:")
	print(f"  Total markets in step_3: {len(markets)}")
	print(f"  Markets meeting criteria (price >= 80% for >= 7 days): {len(filtered_markets)}")
	print(f"  Stored {len(filtered_markets)} markets in step_4 collection")


if __name__ == "__main__":
	main()
