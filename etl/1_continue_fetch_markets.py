import requests
from pymongo import MongoClient, UpdateOne
import time

def continue_fetch_markets(status, start_cursor, initial_count=0, max_markets=10_000_000, collection=None, batch_size=1000, max_retries=5):
    """
    Continue fetching markets from a specific cursor position.
    
    Args:
        status: Market status ('settled' or 'closed')
        start_cursor: Cursor to start from (where previous fetch stopped)
        initial_count: Number of markets already fetched (for display purposes)
        max_markets: Maximum total markets to fetch
        collection: MongoDB collection to write to
        batch_size: Number of markets to batch before writing
        max_retries: Maximum number of retries for failed requests
    """
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    params = {"limit": 1000, "status": status}
    
    session = requests.Session()
    all_markets = []
    buffer = []
    total_fetched = initial_count  # Start from the count we already have
    cursor = start_cursor

    def write_batch(batch, col):
        if col is None or not batch:
            return
        ops = [
            UpdateOne({"ticker": m.get("ticker")}, {"$set": m}, upsert=True)
            for m in batch
        ]
        result = col.bulk_write(ops, ordered=False)
        print(f"Stored batch: {len(batch)} {status} markets "
              f"(matched: {result.matched_count}, upserted: {result.upserted_count})")

    print(f"Continuing fetch from cursor: {cursor[:50]}...")
    print(f"Starting from count: {initial_count}")

    while total_fetched < max_markets:
        if cursor:
            params["cursor"] = cursor
        else:
            params.pop("cursor", None)

        # Retry logic for 500 errors and network issues
        retry_count = 0
        data = None
        while retry_count < max_retries:
            try:
                resp = session.get(url, params=params, timeout=30)
                
                # If 500 error, retry with exponential backoff
                if resp.status_code == 500:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count  # Exponential backoff: 2, 4, 8, 16, 32 seconds
                        print(f"Server error 500, retrying in {wait_time} seconds... (attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Request failed after {max_retries} retries for status='{status}'")
                        print(f"Last cursor: {cursor}")
                        break
                
                resp.raise_for_status()
                data = resp.json()
                break  # Success, exit retry loop
                
            except requests.exceptions.RequestException as e:
                # For other network errors, also retry
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    print(f"Request error: {e}, retrying in {wait_time} seconds... (attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"Request failed after {max_retries} retries for status='{status}': {e}")
                    print(f"Last cursor: {cursor}")
                    break
            except Exception as e:
                print(f"Unexpected error for status='{status}': {e}")
                print(f"Last cursor: {cursor}")
                break

        if data is None:
            print(f"Stopping fetch due to persistent errors. Total fetched: {total_fetched}")
            break

        if "error" in data:
            print(f"API error for status='{status}': {data.get('error')}")
            break

        markets = data.get("markets", [])
        if not markets:
            print("No more markets to fetch.")
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
            print("No more cursor - reached end of markets.")
            break
        
        # Small delay between requests to be respectful to the API
        time.sleep(0.1)

    if buffer:
        write_batch(buffer, collection)

    return all_markets


def main():
    client = MongoClient("mongodb://localhost:27017")
    db = client["tail-end-analysis"]
    col = db["step_11"]

    # Ensure index for fast upsert
    col.create_index("ticker", unique=True)

    # The cursor where the previous fetch stopped
    failed_cursor = "CgsIyZyLxwYQ2N-PLhI1S1hNVkVORkxNVUxUSUdBTUVFWFRFTkRFRC0yMDI1NjY5ODIzRTRGNjktMkUzODRENDc1M0U"
    
    # Number of settled markets already fetched (1,764,000)
    initial_settled_count = 1764000

    print("Continuing fetch of settled markets...")
    print(f"Starting from cursor and count: {initial_settled_count}")
    
    settled_markets = continue_fetch_markets(
        status="settled",
        start_cursor=failed_cursor,
        initial_count=initial_settled_count,
        max_markets=10_000_000,
        collection=col
    )

    print(f"\nCompleted continuation. New settled markets fetched: {len(settled_markets)}")
    print(f"Total settled markets now: {initial_settled_count + len(settled_markets)}")


if __name__ == "__main__":
    main()

