from datetime import datetime
from pymongo import MongoClient


def _parse_iso_utc(ts_str):
	"""Parse ISO8601 timestamp strings that may end with 'Z' into aware datetimes."""
	if not ts_str:
		return None
	try:
		return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
	except Exception:
		return None


def _is_open_longer_than_two_weeks(market):
	open_time_str = market.get("open_time")
	close_time_str = market.get("close_time")
	open_dt = _parse_iso_utc(open_time_str)
	close_dt = _parse_iso_utc(close_time_str)
	if not open_dt or not close_dt:
		return False
	# Two weeks as 14 days using total seconds for accuracy
	duration_seconds = (close_dt - open_dt).total_seconds()
	return duration_seconds >= 14 * 24 * 60 * 60


def main():
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	
	# Read all markets from step_1
	step_1_col = db["step_1"]
	all_markets = list(step_1_col.find())
	print(f"Found {len(all_markets)} markets in step_1")
	
	# Filter: volume > 0 and duration >= 2 weeks
	filtered = [
		m for m in all_markets
		if (m.get("volume") or 0) > 0
		and _is_open_longer_than_two_weeks(m)
	]
	
	print(
		f"Filtering: {len(filtered)} markets with volume>0 and open>=2 weeks "
		f"(out of {len(all_markets)} total)"
	)
	
	# Store filtered markets in step_2
	step_2_col = db["step_2"]
	for m in filtered:
		step_2_col.replace_one({"ticker": m.get("ticker")}, m, upsert=True)
	
	print(f"Stored {len(filtered)} markets into MongoDB collection 'step_2'.")


if __name__ == "__main__":
	main()

