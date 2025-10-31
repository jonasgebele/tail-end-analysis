from datetime import datetime
from pymongo import MongoClient
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def plot_market_price(market, save_path=None):
	"""Plot price over time for a single market"""
	ticker = market.get("ticker", "Unknown")
	title = market.get("title", ticker)
	candlesticks = market.get("candlesticks", [])
	
	if not candlesticks:
		print(f"No candlesticks found for {ticker}")
		return
	
	# Extract timestamps and prices
	times = []
	prices_close = []
	prices_mean = []
	
	for candle in candlesticks:
		end_ts = candle.get("end_period_ts")
		if not end_ts:
			continue
		
		price_obj = candle.get("price", {})
		close_price = price_obj.get("close")
		mean_price = price_obj.get("mean")
		
		# Convert timestamp to datetime
		dt = datetime.fromtimestamp(end_ts)
		times.append(dt)
		
		# Use close price if available, otherwise mean, otherwise None
		if close_price is not None:
			prices_close.append(close_price / 100.0)  # Convert cents to dollars
		else:
			prices_close.append(None)
		
		if mean_price is not None:
			prices_mean.append(mean_price / 100.0)  # Convert cents to dollars
		else:
			prices_mean.append(None)
	
	if not times:
		print(f"No valid timestamps found for {ticker}")
		return
	
	# Create the plot
	fig, ax = plt.subplots(figsize=(12, 6))
	
	# Plot close prices (preferred)
	valid_close = [(t, p) for t, p in zip(times, prices_close) if p is not None]
	if valid_close:
		close_times, close_prices = zip(*valid_close)
		ax.plot(close_times, close_prices, label="Close Price", linewidth=1.5, alpha=0.8)
	
	# Plot mean prices as fallback (if close is not available)
	valid_mean = [(t, p) for t, p in zip(times, prices_mean) if p is not None and prices_close[times.index(t)] is None]
	if valid_mean:
		mean_times, mean_prices = zip(*valid_mean)
		ax.plot(mean_times, mean_prices, label="Mean Price", linewidth=1.5, alpha=0.8, linestyle="--")
	
	ax.set_xlabel("Time", fontsize=12)
	ax.set_ylabel("Price (USD)", fontsize=12)
	ax.set_title(f"{title}\n({ticker})", fontsize=14, fontweight="bold")
	ax.grid(True, alpha=0.3)
	ax.legend()
	
	# Format x-axis dates
	ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
	ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(times) // 10)))
	plt.xticks(rotation=45)
	
	plt.tight_layout()
	
	if save_path:
		plt.savefig(save_path, dpi=150, bbox_inches="tight")
		print(f"Saved plot to {save_path}")
	else:
		plt.show()
	
	plt.close()


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
	
	# Create plots directory
	import os
	plots_dir = "plots"
	os.makedirs(plots_dir, exist_ok=True)
	
	# Plot each market
	for i, market in enumerate(markets, 1):
		ticker = market.get("ticker", f"market_{i}")
		print(f"[{i}/{len(markets)}] Plotting {ticker}...")
		
		save_path = os.path.join(plots_dir, f"{ticker.replace('/', '_')}.png")
		plot_market_price(market, save_path=save_path)
	
	print(f"\nCompleted: Generated {len(markets)} plots in '{plots_dir}' directory")


if __name__ == "__main__":
	main()

