import requests
from datetime import datetime, timedelta
import json
import os



base_url = "https://montandon-eoapi-stage.ifrc.org/stac/collections"
#headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

collections = [
    "desinventar-events", "desinventar-impacts", 
    "emdat-events", "emdat-hazards", "emdat-impacts",
    "gdacs-events", "gdacs-hazards", "gdacs-impacts",
    "gfd-events", "gfd-hazards", "gfd-impacts",
    "glide-events", "glide-hazards", 
    "ibtracs-events", "ibtracs-hazards",
    "idmc-gidd-events", "idmc-gidd-impacts", 
    "idmc-idu-events", "idmc-idu-impacts",
    "ifrcevent-events", "ifrcevent-hazards", "ifrcevent-impacts",
    "pdc-events", "pdc-hazards", "pdc-impacts",
    "usgs-events", "usgs-hazards", "usgs-impacts",
]

today = datetime.utcnow()
start_date = (today - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
end_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")
datetime_range = f"{start_date}/{end_date}"

bbox_europe = "-12,34,40,72"
sortby = "-datetime"

all_first_records = []

for collection in collections:
    url = (
        f"{base_url}/{collection}/items"
        f"?limit=10"
        f"&datetime={datetime_range}"
        f"&bbox={bbox_europe}"
        f"&sortby={sortby}"
    )
    print(f"\nTrying collection: {collection}")
    response = requests.get(url) # headers=headers)
    print("  Status:", response.status_code)
    record = None
    if response.status_code == 200:
        data = response.json()
        items = data.get("features", [])
        print(f"  Found {len(items)} matching records")
        if items:
            record = items[0]  # Save first record
            print("  Example record saved.")
        else:
            print("  No records found for this collection in the selected window.")
    else:
        print("  Response content:", response.text)
    all_first_records.append({
        "collection": collection,
        "record": record
    })




output_folder = os.path.join(os.getcwd(), "all_collections")
os.makedirs(output_folder, exist_ok=True)

output_path = os.path.join(output_folder, "first_records_per_collection.json")
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(all_first_records, f, indent=2)