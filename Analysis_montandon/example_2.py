## counts events by collection and country in the Montandon STAC API
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import time
import random
import json
import os
import requests
event_collections = [
    "ifrcevent-events", "pdc-events", "desinventar-events", "emdat-events",
    "gdacs-events", "gfd-events", "glide-events", "ibtracs-events",
    "idmc-gidd-events", "idmc-idu-events", "reference-events"
]
BASE_URL = "https://montandon-eoapi-stage.ifrc.org/stac/collections"
ERROR_FILE = "event_count_errors.json"

def write_error_entry(error_entry):
    """Append a single error as a JSON string to the error file."""
    # Each line is one valid JSON object; a list can be constructed later.
    with open(ERROR_FILE, "a", encoding="utf-8") as ef:
        ef.write(json.dumps(error_entry, ensure_ascii=False) + "\n")

def fetch_country_counts(collection_id, page_limit=100, max_retries=10):
    country_counter = Counter()
    url = f"{BASE_URL}/{collection_id}/items?limit={page_limit}&fields=id,properties"
    total_fetched = 0
    page_count = 0
    print(f"Started processing: {collection_id}")
    # Random initial sleep to stagger thread starts
    time.sleep(random.uniform(0.1, 0.8))
    while url:
        page_count += 1
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=60)
                if response.status_code == 500:
                    error_entry = {
                        "collection": collection_id,
                        "page": page_count,
                        "reason": "500",
                        "url": url
                    }
                    print(f"!! Server error 500 on {collection_id} (page {page_count}), writing error and giving up.")
                    write_error_entry(error_entry)
                    return collection_id, Counter()
                elif response.status_code != 200:
                    print(f"Error fetching {collection_id} (page {page_count}), attempt {attempt + 1}: HTTP {response.status_code}")
                    raise Exception(f"HTTP {response.status_code}")
                data = response.json()
                features = data.get("features", [])
                total_fetched += len(features)
                for feature in features:
                    codes = feature.get("properties", {}).get("monty:country_codes", [])
                    country_counter.update(codes)
                print(f"  {collection_id} - Page {page_count}: Fetched {len(features)} (cumulative: {total_fetched})")
                next_url = next((l.get("href") for l in data.get("links", []) if l.get("rel") == "next"), None)
                url = next_url
                break  # Success, break out of retry loop
            except Exception as ex:
                wait_time = 2 ** attempt + random.uniform(0, 0.2)
                print(f"  Error on {collection_id} page {page_count}, attempt {attempt + 1} -- {ex}. Retrying in {wait_time:.2f}s.")
                time.sleep(wait_time)
                if attempt == max_retries - 1:
                    error_entry = {
                        "collection": collection_id,
                        "page": page_count,
                        "reason": f"Failed after {max_retries} attempts: {str(ex)}",
                        "url": url
                    }
                    print(f"  Giving up on {collection_id} (page {page_count}) after {max_retries} attempts, error written.")
                    write_error_entry(error_entry)
                    url = None  # Quit this collection
    print(f"Finished processing: {collection_id} (total events: {total_fetched})")
    return collection_id, country_counter

def main():
    results = {}  # collection -> Counter

    # Clear any existing error file before running
    if os.path.exists(ERROR_FILE):
        os.remove(ERROR_FILE)

    print("=== Starting event count by country for all collections ===\n")
    with ThreadPoolExecutor(max_workers=len(event_collections)) as executor:
        future_to_collection = {
            executor.submit(fetch_country_counts, coll, 100, 10): coll
            for coll in event_collections
        }
        for future in as_completed(future_to_collection):
            coll = future_to_collection[future]
            try:
                coll_id, country_counter = future.result()
                results[coll_id] = dict(country_counter)
            except Exception as exc:
                error_entry = {
                    "collection": coll,
                    "reason": str(exc)
                }
                print(f"{coll} generated an exception: {exc}, writing error.")
                write_error_entry(error_entry)

    print("\n=== All collections processed. Results below. ===")
    for coll_id, counter in results.items():
        total = sum(counter.values())
        print(f"\nCollection: {coll_id}")
        print(f"Total events: {total}")
        print("Country counts:")
        for iso3, count in counter.items():
            print(f"    {iso3}: {count}")

    # Save results to CSV
    with open("event_counts_by_country.csv", "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["collection", "iso3_country", "event_count"])
        for coll_id, counter in results.items():
            for iso3, count in counter.items():
                writer.writerow([coll_id, iso3, count])
    print("Results saved to event_counts_by_country.csv")

    if os.path.exists(ERROR_FILE):
        print(f"Errors were encountered and immediately written to {ERROR_FILE}")
    else:
        print("No errors encountered.")

if __name__ == "__main__":
    main()
