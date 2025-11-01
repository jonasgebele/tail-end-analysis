import requests
from pymongo import MongoClient


def fetch_markets_by_status(status, max_markets=10000, collection=None, batch_size=1000):
	url = "https://api.elections.kalshi.com/trade-api/v2/markets"
	params = {
		"limit": 1000,
		"status": status,
	}
	
	all_markets = []
	buffer = []
	total_fetched = 0
	cursor = None
	
	def write_batch(batch, col):
		if col is not None and batch:
			for m in batch:
				col.replace_one({"ticker": m.get("ticker")}, m, upsert=True)
	
	while total_fetched < max_markets:
		if cursor:
			params["cursor"] = cursor
		else:
			params.pop("cursor", None)
		
		try:
			resp = requests.get(url, params=params, timeout=20)
			if not resp.ok:
				print(f"API status {resp.status_code} for status='{status}'")
				break
			data = resp.json()
		except Exception as e:
			print(f"Request failed for status='{status}': {e}")
			break
		
		if "error" in data:
			print(f"API error for status='{status}': {data.get('error')}")
			break
		
		markets = data.get("markets", [])
		
		# Add to buffer and write in batches
		for market in markets:
			if total_fetched >= max_markets:
				break
			buffer.append(market)
			all_markets.append(market)
			total_fetched += 1
			
			# Write batch when buffer reaches batch_size
			if len(buffer) >= batch_size:
				write_batch(buffer, collection)
				print(f"Stored batch of {len(buffer)} {status} markets. Total fetched: {total_fetched}")
				buffer = []
		
		print(f"Fetched {len(markets)} {status} markets. Total: {total_fetched}")
		
		if total_fetched >= max_markets:
			break
		
		cursor = data.get("cursor")
		if not cursor or not markets:
			break
	
	# Write any remaining markets in buffer
	if buffer:
		write_batch(buffer, collection)
		print(f"Stored final batch of {len(buffer)} {status} markets.")
	
	return all_markets


def main():
	client = MongoClient("mongodb://localhost:27017")
	db = client["tail-end-analysis"]
	col = db["step_1"]
	
	# Fetch 10k closed markets and write in batches
	print("Fetching closed markets...")
	closed_markets = fetch_markets_by_status("closed", max_markets=10000, collection=col, batch_size=1000)
	
	# Fetch 10k settled markets and write in batches
	print("\nFetching settled markets...")
	settled_markets = fetch_markets_by_status("settled", max_markets=10000, collection=col, batch_size=1000)
	
	# Combine both for final count
	all_markets = closed_markets + settled_markets
	
	print(f"\nCompleted: {len(closed_markets)} closed and {len(settled_markets)} settled markets ({len(all_markets)} total) stored in MongoDB collection 'step_1'.")


if __name__ == "__main__":
	main()

