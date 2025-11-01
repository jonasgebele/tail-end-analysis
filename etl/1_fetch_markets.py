import requests
from pymongo import MongoClient, UpdateOne

def fetch_markets_by_status(status, max_markets=10000, collection=None, batch_size=1000):
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    params = {"limit": 1000, "status": status}
    
    session = requests.Session()  # ✅ reuse TCP connections
    all_markets = []
    buffer = []
    total_fetched = 0
    cursor = None

    def write_batch(batch, col):
        if col is None or not batch:
            return
        # ✅ Bulk upsert using a single round-trip
        ops = [
            UpdateOne({"ticker": m.get("ticker")}, {"$set": m}, upsert=True)
            for m in batch
        ]
        result = col.bulk_write(ops, ordered=False)
        print(f"Stored batch: {len(batch)} {status} markets "
              f"(matched: {result.matched_count}, upserted: {result.upserted_count})")

    while total_fetched < max_markets:
        if cursor:
            params["cursor"] = cursor
        else:
            params.pop("cursor", None)

        try:
            resp = session.get(url, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Request failed for status='{status}': {e}")
            break

        if "error" in data:
            print(f"API error for status='{status}': {data.get('error')}")
            break

        markets = data.get("markets", [])
        if not markets:
            break

        for market in markets:
            if total_fetched >= max_markets:
                break
            buffer.append(market)
            all_markets.append(market)
            total_fetched += 1

            if len(buffer) >= batch_size:
                write_batch(buffer, collection)
                buffer.clear()

        print(f"Fetched {len(markets)} {status} markets. Total fetched: {total_fetched}")
        cursor = data.get("cursor")
        if not cursor:
            break

    if buffer:
        write_batch(buffer, collection)

    return all_markets


def main():
    client = MongoClient("mongodb://localhost:27017")
    db = client["tail-end-analysis"]
    col = db["step_11"]

    # ✅ Ensure index for fast upsert
    col.create_index("ticker", unique=True)

    print("Fetching closed markets...")
    closed_markets = fetch_markets_by_status("closed", max_markets=10_000_000, collection=col)

    print("\nFetching settled markets...")
    settled_markets = fetch_markets_by_status("settled", max_markets=10_000_000, collection=col)

    print(f"\nCompleted: {len(closed_markets)} closed and {len(settled_markets)} settled markets.")

if __name__ == "__main__":
    main()
