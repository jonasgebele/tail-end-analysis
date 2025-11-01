from datetime import datetime
from pymongo import MongoClient
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os


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


def calculate_apy_over_time(market, price_type='close'):
	"""Calculate APY over time assuming market resolves to 1.00 USD
	price_type can be 'close', 'yes_ask', or 'yes_bid'
	"""
	candlesticks = market.get("candlesticks", [])
	
	if not candlesticks:
		return None, None
	
	# Sort candlesticks by timestamp
	sorted_candles = sorted(candlesticks, key=lambda x: x.get("end_period_ts", 0))
	
	# Get last date (resolution date)
	last_candle = sorted_candles[-1]
	last_ts = last_candle.get("end_period_ts")
	if not last_ts:
		return None, None
	
	last_date = datetime.fromtimestamp(last_ts)
	
	# Calculate APY for each point in time
	times = []
	apy_values = []
	
	for candle in sorted_candles:
		end_ts = candle.get("end_period_ts")
		if not end_ts:
			continue
		
		current_date = datetime.fromtimestamp(end_ts)
		
		# Get price based on price_type
		if price_type == 'close':
			price_obj = candle.get("price", {})
			price_val = price_obj.get("close")
		elif price_type == 'yes_ask':
			yes_ask_obj = candle.get("yes_ask", {})
			price_val = yes_ask_obj.get("close")
		elif price_type == 'yes_bid':
			yes_bid_obj = candle.get("yes_bid", {})
			price_val = yes_bid_obj.get("close")
		else:
			continue
		
		if price_val is None or price_val <= 0:
			continue
		
		price = price_val / 100.0  # Convert cents to dollars
		
		# Calculate days remaining until resolution
		days_remaining = (last_date - current_date).days
		if days_remaining <= 0:
			# Skip if we're at or past the resolution date
			continue
		
		# Calculate APY: ((Final Price / Current Price) - 1) * (365 / days_remaining) * 100
		# Assuming final price is 1.00 USD when resolved
		final_price = 1.00
		apy = ((final_price / price) - 1) * (365 / days_remaining) * 100
		
		times.append(current_date)
		apy_values.append(apy)
	
	if not times:
		return None, None
	
	return times, apy_values


def plot_apy(market, save_path=None):
	"""Plot price and APY over time for a single market - combined plot"""
	ticker = market.get("ticker", "Unknown")
	title = market.get("title", ticker)
	candlesticks = market.get("candlesticks", [])
	
	if not candlesticks:
		print(f"No candlesticks found for {ticker}")
		return
	
	# Calculate APY over time (will recalculate for all three types later, but check if close works)
	apy_times, apy_values = calculate_apy_over_time(market, price_type='close')
	
	if apy_times is None:
		print(f"Could not calculate APY for {ticker}")
		return
	
	# Extract price data (close, yes_ask, yes_bid)
	price_times = []
	prices_close = []
	yes_ask_close = []
	yes_bid_close = []
	
	for candle in candlesticks:
		end_ts = candle.get("end_period_ts")
		if not end_ts:
			continue
		
		# Convert timestamp to datetime
		dt = datetime.fromtimestamp(end_ts)
		price_times.append(dt)
		
		# Extract close price
		price_obj = candle.get("price", {})
		close_price = price_obj.get("close")
		if close_price is not None:
			prices_close.append(close_price / 100.0)  # Convert cents to dollars
		else:
			prices_close.append(None)
		
		# Extract yes_ask close
		yes_ask_obj = candle.get("yes_ask", {})
		yes_ask_close_val = yes_ask_obj.get("close")
		if yes_ask_close_val is not None:
			yes_ask_close.append(yes_ask_close_val / 100.0)  # Convert cents to dollars
		else:
			yes_ask_close.append(None)
		
		# Extract yes_bid close
		yes_bid_obj = candle.get("yes_bid", {})
		yes_bid_close_val = yes_bid_obj.get("close")
		if yes_bid_close_val is not None:
			yes_bid_close.append(yes_bid_close_val / 100.0)  # Convert cents to dollars
		else:
			yes_bid_close.append(None)
	
	if not price_times:
		print(f"No valid timestamps found for {ticker}")
		return
	
	# Calculate APY for close and ask price types
	apy_times_close, apy_values_close = calculate_apy_over_time(market, price_type='close')
	apy_times_ask, apy_values_ask = calculate_apy_over_time(market, price_type='yes_ask')
	
	# Create figure with two subplots stacked vertically
	fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
	
	# Top subplot: Price over time (3 lines)
	valid_close = [(t, p) for t, p in zip(price_times, prices_close) if p is not None]
	if valid_close:
		close_times, close_prices = zip(*valid_close)
		ax1.plot(close_times, close_prices, label="Close Price", linewidth=1.5, alpha=0.8, color='steelblue')
	
	valid_yes_ask = [(t, p) for t, p in zip(price_times, yes_ask_close) if p is not None]
	if valid_yes_ask:
		yes_ask_times, yes_ask_prices = zip(*valid_yes_ask)
		ax1.plot(yes_ask_times, yes_ask_prices, label="Yes Ask Close", linewidth=1.5, alpha=0.8, color='green', linestyle='--')
	
	valid_yes_bid = [(t, p) for t, p in zip(price_times, yes_bid_close) if p is not None]
	if valid_yes_bid:
		yes_bid_times, yes_bid_prices = zip(*valid_yes_bid)
		ax1.plot(yes_bid_times, yes_bid_prices, label="Yes Bid Close", linewidth=1.5, alpha=0.8, color='red', linestyle=':')
	
	ax1.set_ylabel("Price (USD)", fontsize=12)
	ax1.set_title(f"{title}\n({ticker})", fontsize=14, fontweight="bold")
	ax1.grid(True, alpha=0.3)
	ax1.legend()
	
	# Bottom subplot: APY over time (2 lines)
	if apy_times_close and apy_values_close:
		ax2.plot(apy_times_close, apy_values_close, label="APY (Close)", linewidth=1.5, alpha=0.8, color='steelblue')
	
	if apy_times_ask and apy_values_ask:
		ax2.plot(apy_times_ask, apy_values_ask, label="APY (Yes Ask)", linewidth=1.5, alpha=0.8, color='green', linestyle='--')
	
	ax2.legend()
	
	ax2.set_xlabel("Time", fontsize=12)
	ax2.set_ylabel("APY (%)", fontsize=12)
	ax2.grid(True, alpha=0.3)
	
	# Format x-axis dates (only on bottom subplot since sharex=True)
	ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
	ax2.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=15))
	ax2.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
	plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
	
	# Add a horizontal line at 0% APY for reference
	ax2.axhline(y=0, color='black', linestyle='--', linewidth=0.5, alpha=0.5)
	
	plt.tight_layout()
	
	if save_path:
		plt.savefig(save_path, dpi=150, bbox_inches="tight")
		print(f"Saved plot to {save_path}")
	else:
		plt.show()
	
	plt.close()


def plot_all_apy_summary(markets, save_path=None):
	"""Create a summary plot showing APY over time for all markets"""
	fig, ax = plt.subplots(figsize=(14, 8))
	
	# Collect all market APY data (using ask price)
	market_data = []
	valid_count = 0
	for market in markets:
		ticker = market.get("ticker", "Unknown")
		times, apy_values = calculate_apy_over_time(market, price_type='yes_ask')
		
		if times is not None and apy_values is not None:
			ax.plot(times, apy_values, alpha=0.5, linewidth=1, label=ticker)
			market_data.append((times, apy_values))
			valid_count += 1
	
	if valid_count == 0:
		print("No valid APY data to plot")
		return
	
	# Calculate lowest market APY at each time point
	# Collect all unique timestamps
	all_timestamps = set()
	for times, _ in market_data:
		all_timestamps.update(times)
	
	all_timestamps = sorted(all_timestamps)
	
	# For each timestamp, collect APY values from all markets that have data at that time
	lowest_market_times = []
	lowest_market_values = []
	
	for ts in all_timestamps:
		apy_at_ts = []
		for times, apy_values in market_data:
			# Find the closest timestamp in this market's data
			if ts in times:
				idx = times.index(ts)
				apy_at_ts.append(apy_values[idx])
		
		if len(apy_at_ts) >= 1:
			# Take the lowest value
			lowest_apy = min(apy_at_ts)
			lowest_market_times.append(ts)
			lowest_market_values.append(lowest_apy)
	
	# Plot the lowest market line (bolder)
	lowest_line = None
	if lowest_market_times and lowest_market_values:
		lowest_line, = ax.plot(lowest_market_times, lowest_market_values, 
				label="Lowest Market", 
				linewidth=3, alpha=0.9, color='darkred', linestyle='-')
	
	ax.set_xlabel("Time", fontsize=12)
	ax.set_ylabel("APY (%)", fontsize=12)
	ax.set_title("APY Over Time for All Markets (Ask Price)\n(Assuming resolution at $1.00)", 
				 fontsize=14, fontweight="bold")
	ax.set_ylim(0, 100)  # Focus on 0-100% APY region
	ax.grid(True, alpha=0.3)
	
	# Format x-axis dates
	ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
	ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=15))
	plt.xticks(rotation=45)
	
	# Add a horizontal line at 0% APY for reference
	ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5, alpha=0.5)
	
	# Don't show legend if too many markets (would be cluttered)
	# But always show the lowest market line legend
	if valid_count <= 10:
		ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
	elif lowest_line is not None:
		# Show only the lowest market line legend if too many markets
		ax.legend(handles=[lowest_line], labels=["Lowest Market"], 
				 bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
	
	plt.tight_layout()
	
	if save_path:
		plt.savefig(save_path, dpi=150, bbox_inches="tight")
		print(f"Saved summary plot to {save_path}")
	else:
		plt.show()
	
	plt.close()


def main():
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	
	# Read all markets from step_4
	step_4_col = db["step_4"]
	markets = list(step_4_col.find())
	print(f"Found {len(markets)} markets in step_4")
	
	if not markets:
		print("No markets found in step_4. Exiting.")
		return
	
	# Create plots/apy directory
	plots_dir = os.path.join("plots", "apy")
	os.makedirs(plots_dir, exist_ok=True)
	
	# Plot APY for each market
	valid_markets = []
	for i, market in enumerate(markets, 1):
		ticker = market.get("ticker", f"market_{i}")
		times, apy_values = calculate_apy_over_time(market, price_type='close')
		
		if times is None:
			print(f"[{i}/{len(markets)}] ✗ {ticker}: Could not calculate APY")
			continue
		
		valid_markets.append(market)
		if apy_values:
			max_apy = max(apy_values)
			min_apy = min(apy_values)
			print(f"[{i}/{len(markets)}] ✓ {ticker}: APY range (close) = {min_apy:.2f}% to {max_apy:.2f}%")
		
		save_path = os.path.join(plots_dir, f"{ticker.replace('/', '_')}_apy.png")
		plot_apy(market, save_path=save_path)
	
	# Create summary plot
	if valid_markets:
		summary_path = os.path.join(plots_dir, "apy_summary.png")
		plot_all_apy_summary(valid_markets, save_path=summary_path)
	
	print(f"\nCompleted:")
	print(f"  Total markets: {len(markets)}")
	print(f"  Markets with valid APY: {len(valid_markets)}")
	print(f"  Generated {len(valid_markets)} individual APY plots")
	print(f"  Generated 1 summary plot")
	print(f"  All plots saved to '{plots_dir}' directory")


if __name__ == "__main__":
	main()

