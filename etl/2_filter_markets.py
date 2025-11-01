from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm


def _parse_iso_utc(ts_str):
	"""Parse ISO8601 timestamp strings that may end with 'Z' into aware datetimes."""
	if not ts_str:
		return None
	try:
		return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
	except Exception:
		return None


def _is_open_longer_than_one_month(market):
	open_time_str = market.get("open_time")
	close_time_str = market.get("close_time")
	open_dt = _parse_iso_utc(open_time_str)
	close_dt = _parse_iso_utc(close_time_str)
	if not open_dt or not close_dt:
		return False
	# One month as 30 days using total seconds for accuracy
	duration_seconds = (close_dt - open_dt).total_seconds()
	return duration_seconds >= 30 * 24 * 60 * 60


def main():
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	
	# Read all markets from step_1
	step_1_col = db["step_11"]
	print("Loading markets from MongoDB...")
	all_markets = list(step_1_col.find())
	print(f"Found {len(all_markets)} markets in step_11")
	
	# Filter: volume > 0 and duration >= 1 month
	print("Filtering markets...")
	filtered = [
		m for m in tqdm(all_markets, desc="Filtering", unit="market")
		if (m.get("volume") or 0) > 0
		and _is_open_longer_than_one_month(m)
	]
	
	print(
		f"Filtered: {len(filtered)} markets with volume>0 and open>=1 month "
		f"(out of {len(all_markets)} total)"
	)
	
	# Store filtered markets in step_2
	step_2_col = db["step_22"]
	print("Storing filtered markets to MongoDB...")
	for m in tqdm(filtered, desc="Storing", unit="market"):
		step_2_col.replace_one({"ticker": m.get("ticker")}, m, upsert=True)
	
	print(f"Stored {len(filtered)} markets into MongoDB collection 'step_22'.")


if __name__ == "__main__":
	main()

