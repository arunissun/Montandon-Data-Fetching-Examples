# Get total events in usgs-events and county of events by country  

import requests
from collections import Counter
import csv
import time
import random
import json
import os

# We are focusing only on the usgs-events collection
COLLECTION_ID = "usgs-events"
BASE_URL = "https://montandon-eoapi-stage.ifrc.org/stac/collections"
ERROR_FILE = "event_count_errors_usgs.json"
OUTPUT_FILE = "event_counts_by_country_usgs.csv"

def write_error_entry(error_entry):
    """Append a single error as a JSON string to the error file."""
    with open(ERROR_FILE, "a", encoding="utf-8") as ef:
        ef.write(json.dumps(error_entry, ensure_ascii=False) + "\n")

def fetch_country_counts(collection_id, page_limit=250, max_retries=5):
    """
    Fetches all items from a collection, paginating through the results,
    and counts the occurrences of each country code.
    """
    country_counter = Counter()
    # Only request the fields we actually need to reduce response size
    url = f"{BASE_URL}/{collection_id}/items?limit={page_limit}&fields=properties.monty:country_codes"
    
    page_count = 0
    total_fetched = 0
    
    print(f"Started processing: {collection_id}")

    # Use a session for connection pooling, which is more efficient
    with requests.Session() as session:
        while url:
            page_count += 1
            for attempt in range(max_retries):
                try:
                    response = session.get(url, timeout=90)
                    response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
                    
                    data = response.json()
                    features = data.get("features", [])
                    total_fetched += len(features)
                    
                    # Update the counter with country codes from the current page
                    for feature in features:
                        codes = feature.get("properties", {}).get("monty:country_codes", [])
                        country_counter.update(codes)
                        
                    print(f"  {collection_id} - Page {page_count}: Fetched {len(features)} items (Total: {total_fetched})")
                    
                    # Find the URL for the next page
                    url = next((link.get("href") for link in data.get("links", []) if link.get("rel") == "next"), None)
                    
                    # Successfully processed page, break the retry loop
                    break
                
                except requests.exceptions.RequestException as ex:
                    wait_time = 2 ** attempt
                    print(f"  Error on {collection_id} page {page_count}, attempt {attempt + 1}: {ex}. Retrying in {wait_time}s.")
                    time.sleep(wait_time)
                    if attempt == max_retries - 1:
                        error_entry = {
                            "collection": collection_id,
                            "page": page_count,
                            "reason": f"Failed after {max_retries} attempts: {str(ex)}",
                            "url": url
                        }
                        print(f"  Giving up on {collection_id} page {page_count}. Error logged.")
                        write_error_entry(error_entry)
                        url = None  # Stop processing this collection
    
    print(f"Finished processing: {collection_id} (total events processed: {total_fetched})")
    return country_counter

def main():
    # Clear any existing error file before running
    if os.path.exists(ERROR_FILE):
        os.remove(ERROR_FILE)

    print(f"=== Starting event count by country for {COLLECTION_ID} ===\n")
    
    country_counts = fetch_country_counts(COLLECTION_ID)

    # Save to CSV
    if country_counts:
        with open(OUTPUT_FILE, "w", newline='', encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["country_code", "event_count"])
            for country, count in country_counts.most_common():
                writer.writerow([country, count])
        print(f"\nResults saved to {OUTPUT_FILE}")
    else:
        print("\nNo data was processed.")

    if os.path.exists(ERROR_FILE):
        print(f"Errors were encountered and have been logged to {ERROR_FILE}")
    else:
        print("Processing completed with no errors.")

if __name__ == "__main__":
    main()
