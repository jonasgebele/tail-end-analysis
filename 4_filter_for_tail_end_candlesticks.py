from datetime import datetime
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


def has_price_above_threshold_for_last_n_days(candlesticks, threshold=0.8, min_duration_days=7):
	"""Check if the last N days of the market have price >= threshold.
	
	This checks specifically the final days of the market's lifetime, not any arbitrary period.
	"""
	if not candlesticks:
		return False
	
	# Sort candlesticks by timestamp (oldest first)
	sorted_candles = sorted(candlesticks, key=lambda x: x.get("end_period_ts", 0))
	
	# Filter out candles without valid timestamps or prices
	valid_candles = []
	for candle in sorted_candles:
		end_ts = candle.get("end_period_ts")
		if not end_ts:
			continue
		price = get_price_from_candle(candle)
		if price is None:
			continue
		valid_candles.append({
			"timestamp": end_ts,
			"price": price,
			"datetime": datetime.fromtimestamp(end_ts)
		})
	
	if not valid_candles:
		return False
	
	# Get the last N candles (since we're using daily candles, this represents the last N days)
	# We need at least min_duration_days candles to check
	if len(valid_candles) < min_duration_days:
		return False
	
	# Take the last min_duration_days candles
	last_n_candles = valid_candles[-min_duration_days:]
	
	# Check if all of the last N candles are above threshold
	for candle in last_n_candles:
		if candle["price"] < threshold:
			return False
	
	return True


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
	
	# Filter markets: price >= threshold for the last N days
	THRESHOLD = 0.95  # 95%
	MIN_DURATION_DAYS = 7  # Last 7 days
	
	filtered_markets = []
	
	for i, market in enumerate(markets, 1):
		ticker = market.get("ticker", f"market_{i}")
		candlesticks = market.get("candlesticks", [])
		
		if not candlesticks:
			continue
		
		if has_price_above_threshold_for_last_n_days(candlesticks, threshold=THRESHOLD, min_duration_days=MIN_DURATION_DAYS):
			filtered_markets.append(market)
			print(f"[{i}/{len(markets)}] ✓ {ticker}: price >= {THRESHOLD*100:.0f}% for last {MIN_DURATION_DAYS} days")
		else:
			print(f"[{i}/{len(markets)}] ✗ {ticker}: does not meet criteria (last {MIN_DURATION_DAYS} days)")
	
	# Store filtered markets in step_4
	step_4_col = db["step_4"]
	for market in filtered_markets:
		ticker = market.get("ticker")
		step_4_col.replace_one({"ticker": ticker}, market, upsert=True)
	
	print(f"\nCompleted:")
	print(f"  Total markets in step_3: {len(markets)}")
	print(f"  Markets meeting criteria (price >= {THRESHOLD*100:.0f}% for last {MIN_DURATION_DAYS} days): {len(filtered_markets)}")
	print(f"  Stored {len(filtered_markets)} markets in step_4 collection")


if __name__ == "__main__":
	main()
