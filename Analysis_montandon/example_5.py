import requests
import pandas as pd
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime

# --- Configuration ---
BASE_URL = "https://montandon-eoapi-stage.ifrc.org/stac"
OUTPUT_FILE = "hazard_counts_by_year_and_type.xlsx" # Changed to .xlsx
START_YEAR = 1800
INTERVAL_YEARS = 50
MAX_WORKERS = 10 # Number of parallel requests

def get_hazard_collections(session):
    """Retrieves a list of all collection IDs that have '-events' in their name."""
    print("Fetching list of all collections...")
    url = f"{BASE_URL}/collections"
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Filter for collections that have hazard codes in their summaries
        hazard_collections = []
        for collection in data.get("collections", []):
            coll_id = collection.get("id", "unknown")
            summaries = collection.get("summaries", {})
            hazard_codes = summaries.get("monty:hazard_codes")
            
            print(f"  Checking collection '{coll_id}'")
            if "-events" in coll_id:
                hazard_collections.append(coll_id)
                print(f"    ✓ Has '-events' in name - Added")
            else:
                print(f"    ✗ No '-events' in name")
        
        print(f"\nFound {len(hazard_collections)} collections with hazard data: {hazard_collections}\n")
        return hazard_collections
    except requests.exceptions.RequestException as e:
        print(f"FATAL: Could not fetch the list of collections. {e}")
        return []

def generate_time_bins(start_year, interval):
    """Generates 50-year time bins from the start year to today."""
    bins = []
    current_year = start_year
    end_year = datetime.now().year

    while current_year <= end_year:
        bin_start = current_year
        bin_end = current_year + interval - 1
        # Ensure the last bin does not go into the future
        if bin_end > end_year:
            bin_end = end_year
        
        start_str = f"{bin_start}-01-01T00:00:00Z"
        end_str = f"{bin_end}-12-31T23:59:59Z"
        
        bins.append({
            "label": f"{bin_start}-{bin_end}",
            "start_datetime": start_str,
            "end_datetime": end_str
        })
        current_year += interval
    return bins

def fetch_counts_for_bin(session, collection_id, time_bin):
    """
    Fetches all items for a specific collection and time bin, and counts
    the occurrences of each hazard code.
    """
    bin_label = time_bin['label']
    datetime_range = f"{time_bin['start_datetime']}/{time_bin['end_datetime']}"
    
    print(f"  -> Starting: {collection_id} for period {bin_label}")
    
    hazard_counter = Counter()
    url = (
        f"{BASE_URL}/collections/{collection_id}/items"
        f"?limit=250&datetime={datetime_range}&fields=properties.monty:hazard_codes"
    )
    
    items_fetched = 0
    while url:
        try:
            response = session.get(url, timeout=90)
            response.raise_for_status()
            data = response.json()
            
            features = data.get("features", [])
            if not features and items_fetched == 0:
                # No items found in this bin for this collection, exit early
                print(f"  -> Completed: {collection_id} for {bin_label} (0 items)")
                return collection_id, bin_label, Counter()

            items_fetched += len(features)

            for feature in features:
                codes = feature.get("properties", {}).get("monty:hazard_codes", [])
                hazard_counter.update(codes)
            
            url = next((link.get("href") for link in data.get("links", []) if link.get("rel") == "next"), None)

        except requests.exceptions.RequestException as e:
            print(f"  -> ERROR on {collection_id} for {bin_label}: {e}")
            # Return an empty counter on error to avoid partial results
            return collection_id, bin_label, Counter()

    print(f"  -> Completed: {collection_id} for {bin_label} ({items_fetched} items processed)")
    return collection_id, bin_label, hazard_counter

def main():
    """
    Main function to orchestrate fetching counts for all hazard collections
    across all time bins and saving the results.
    """
    with requests.Session() as session:
        hazard_collections = get_hazard_collections(session)
        if not hazard_collections:
            return
            
        time_bins = generate_time_bins(START_YEAR, INTERVAL_YEARS)
        print(f"Generated {len(time_bins)} time bins to process.\n")

        # Create a list of all tasks to run
        tasks = []
        for collection_id in hazard_collections:
            for bin_info in time_bins:
                tasks.append((collection_id, bin_info))

        all_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {
                executor.submit(fetch_counts_for_bin, session, coll_id, t_bin): (coll_id, t_bin['label'])
                for coll_id, t_bin in tasks
            }

            for future in as_completed(future_to_task):
                try:
                    collection_id, bin_label, counts = future.result()
                    if counts:
                        for hazard_code, count in counts.items():
                            all_results.append({
                                "collection": collection_id,
                                "time_period": bin_label,
                                "hazard_code": hazard_code,
                                "event_count": count
                            })
                except Exception as exc:
                    task_id = future_to_task[future]
                    print(f"A task {task_id} generated an exception: {exc}")

    if not all_results:
        print("\nNo hazard data was found for any collection in the specified time periods.")
        return

    # Convert the raw results into a DataFrame
    df_long = pd.DataFrame(all_results)
    
    # Get a sorted list of unique time periods to ensure correct column order
    time_period_order = sorted(df_long['time_period'].unique())

    print(f"\n\nProcessing complete. Writing data to Excel file: {OUTPUT_FILE}")

    # Use ExcelWriter to save multiple sheets to one file
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        # Get unique collection names
        collections = df_long['collection'].unique()
        
        for collection in collections:
            print(f"  -> Creating sheet for {collection}")
            # Filter data for the current collection
            df_collection = df_long[df_long['collection'] == collection]
            
            # Pivot the table to get the desired wide format
            df_pivot = df_collection.pivot_table(
                index='hazard_code',
                columns='time_period',
                values='event_count',
                fill_value=0  # Use 0 for missing hazard/time period combinations
            )
            
            # Ensure all time periods are present as columns in the correct order
            df_pivot = df_pivot.reindex(columns=time_period_order, fill_value=0)
            
            # Write the pivoted DataFrame to a sheet named after the collection
            df_pivot.to_excel(writer, sheet_name=collection)

    print(f"\nSuccessfully created Excel file with {len(collections)} sheets.")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
