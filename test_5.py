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
	yes_ask_close = []
	yes_bid_close = []
	
	for candle in candlesticks:
		end_ts = candle.get("end_period_ts")
		if not end_ts:
			continue
		
		# Convert timestamp to datetime
		dt = datetime.fromtimestamp(end_ts)
		times.append(dt)
		
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
	
	if not times:
		print(f"No valid timestamps found for {ticker}")
		return
	
	# Create the plot
	fig, ax = plt.subplots(figsize=(12, 6))
	
	# Plot close prices
	valid_close = [(t, p) for t, p in zip(times, prices_close) if p is not None]
	if valid_close:
		close_times, close_prices = zip(*valid_close)
		ax.plot(close_times, close_prices, label="Close Price", linewidth=1.5, alpha=0.8, color='steelblue')
	
	# Plot yes_ask close
	valid_yes_ask = [(t, p) for t, p in zip(times, yes_ask_close) if p is not None]
	if valid_yes_ask:
		yes_ask_times, yes_ask_prices = zip(*valid_yes_ask)
		ax.plot(yes_ask_times, yes_ask_prices, label="Yes Ask Close", linewidth=1.5, alpha=0.8, color='green', linestyle='--')
	
	# Plot yes_bid close
	valid_yes_bid = [(t, p) for t, p in zip(times, yes_bid_close) if p is not None]
	if valid_yes_bid:
		yes_bid_times, yes_bid_prices = zip(*valid_yes_bid)
		ax.plot(yes_bid_times, yes_bid_prices, label="Yes Bid Close", linewidth=1.5, alpha=0.8, color='red', linestyle=':')
	
	ax.set_xlabel("Time", fontsize=12)
	ax.set_ylabel("Price (USD)", fontsize=12)
	ax.set_title(f"{title}\n({ticker})", fontsize=14, fontweight="bold")
	ax.grid(True, alpha=0.3)
	ax.legend()
	
	# Format x-axis dates - show more dates
	ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
	# Use AutoDateLocator to automatically pick appropriate intervals, or show more dates
	ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=15))
	ax.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
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
	
	# Read all markets from step_4
	step_4_col = db["step_4"]
	markets = list(step_4_col.find())
	print(f"Found {len(markets)} markets in step_4")
	
	if not markets:
		print("No markets found in step_4. Exiting.")
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

