### code to find the oldest event in every collection 
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
import random
import csv
import json
import os

event_collections = [
    "ifrcevent-events", "pdc-events", "desinventar-events", "emdat-events",
    "gdacs-events", "gfd-events", "glide-events", "ibtracs-events",
    "idmc-gidd-events", "idmc-idu-events", "reference-events", "usgs-events"
]
BASE_URL = "https://montandon-eoapi-stage.ifrc.org/stac/collections"
ERROR_FILE = "oldest_events_errors.json"

def write_error_entry(error_entry):
    with open(ERROR_FILE, "a", encoding="utf-8") as ef:
        ef.write(json.dumps(error_entry, ensure_ascii=False) + "\n")

def is_valid_datetime(dt_str, collection_id=None):
    """
    Validates if a datetime string represents a realistic date.
    Returns True if the date is valid and reasonable, False otherwise.
    Different validation rules for different collections.
    """
    if not dt_str:
        return False
    
    # For desinventar-events: year > 1100 and day != 1
    if collection_id == "desinventar-events":
        try:
            # Parse the datetime
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            
            # Check if the year is reasonable (above 1100 and up to current year + 1)
            current_year = datetime.now().year
            if dt.year <= 1100 or dt.year > current_year + 1:
                return False
            
            # Check if month is valid (1-12)
            if dt.month < 1 or dt.month > 12:
                return False
            
            # Check if day is valid for the given month/year
            import calendar
            max_days = calendar.monthrange(dt.year, dt.month)[1]
            if dt.day < 1 or dt.day > max_days:
                return False
            
            # Additional constraint: day should not be 01
            if dt.day == 1:
                return False
            
            return True
            
        except (ValueError, Exception):
            return False
    
    # For all other collections: just basic validation (year > 100)
    else:
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            current_year = datetime.now().year
            # Basic check: year should be reasonable (above 100)
            if dt.year <= 100 or dt.year > current_year + 1:
                return False
            return True
        except (ValueError, Exception):
            return False

def fetch_oldest_event(collection_id, page_limit=10, max_retries=5):
    """
    Fetches the oldest event from a collection with a valid datetime.
    """
    # Use `sortby` to ask the API to return items in ascending chronological order.
    url = f"{BASE_URL}/{collection_id}/items?limit={page_limit}&sortby=%2Bdatetime"
    
    print(f"Searching for the oldest valid event in: {collection_id}")

    page_count = 0
    while url:
        page_count += 1
        print(f"  -> Fetching page {page_count} for {collection_id}...")
        
        try:
            # Use a session for connection pooling and better performance
            with requests.Session() as session:
                response = session.get(url, timeout=60)
                response.raise_for_status()  # Raises an HTTPError for bad responses

            data = response.json()
            features = data.get("features", [])

            if not features and page_count == 1:
                print(f"  -> No events found for {collection_id}")
                return collection_id, None

            # Iterate through the sorted features to find the first valid one
            for feature in features:
                props = feature.get("properties", {})
                dt_str = props.get("datetime")
                
                # Check if the datetime is valid and reasonable
                if not is_valid_datetime(dt_str, collection_id):
                    print(f"  -> Skipping event {feature.get('id')} with invalid date: {dt_str}")
                    continue

                # The first valid event is the one we want
                oldest_event = {
                    "collection": collection_id,
                    "event_id": feature.get("id"),
                    "datetime": dt_str,
                    "title": props.get("title", ""),
                    "description": props.get("description", "")
                }
                print(f"  -> Success for {collection_id}: Found oldest valid event from {dt_str}")
                return collection_id, oldest_event

            # If we've gone through the whole page and all were invalid, get the next page URL
            url = next((l.get("href") for l in data.get("links", []) if l.get("rel") == "next"), None)

        except requests.exceptions.HTTPError as ex:
            if ex.response.status_code == 500:
                print(f"!! 500 Server Error on {collection_id}. Giving up.")
                write_error_entry({"collection": collection_id, "reason": "500 Server Error", "url": url})
                return collection_id, None # Give up on this collection
            
            # For other HTTP errors, we can just stop processing this collection
            print(f"  HTTP Error on {collection_id} (HTTP {ex.response.status_code}). Stopping search for this collection.")
            write_error_entry({"collection": collection_id, "reason": f"HTTP {ex.response.status_code}", "url": url})
            return collection_id, None

        except Exception as ex:
            # General exception, retry logic might be useful here for network issues
            print(f"  An unexpected error occurred on {collection_id}: {ex}. Stopping search.")
            write_error_entry({"collection": collection_id, "reason": str(ex), "url": url})
            return collection_id, None

    # If the loop finishes, it means no valid events were found
    print(f"  -> No valid events found for {collection_id} after checking all pages.")
    return collection_id, None

def main():
    results = {}   # collection -> event dict

    # Clear previous error file
    if os.path.exists(ERROR_FILE):
        os.remove(ERROR_FILE)

    print("=== Starting oldest event search for all collections ===\n")
    with ThreadPoolExecutor(max_workers=len(event_collections)) as executor:
        future_to_collection = {
            executor.submit(fetch_oldest_event, coll): coll
            for coll in event_collections
        }
        for future in as_completed(future_to_collection):
            coll = future_to_collection[future]
            try:
                coll_id, oldest_event = future.result()
                results[coll_id] = oldest_event
            except Exception as exc:
                error_entry = {"collection": coll, "reason": str(exc)}
                print(f"{coll} generated an exception: {exc}, writing error.")
                write_error_entry(error_entry)

    # Save to CSV
    with open("oldest_events_by_collection.csv", "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["collection", "event_id", "datetime", "title", "description"])
        for coll_id, event in results.items():
            if event:
                writer.writerow([coll_id, event["event_id"], event["datetime"], event["title"], event["description"]])
            else:
                writer.writerow([coll_id, "", "", "", ""])

    print("Results saved to oldest_events_by_collection.csv")
    if os.path.exists(ERROR_FILE):
        print(f"Errors were encountered and immediately written to {ERROR_FILE}")
    else:
        print("No errors encountered.")

if __name__ == "__main__":
    main()
