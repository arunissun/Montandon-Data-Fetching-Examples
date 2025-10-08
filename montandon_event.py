
#import requests
#import json


#collections_with_full_year = ["glide-events", "gdacs-events"]
#usgs_collection = "usgs-events"
#
#start_date_full = "2024-01-01T00:00:00Z"
#end_date_full = "2024-06-30T23:59:59Z"
#
## For USGS, pick one month (example: June 2024)
#start_date_usgs = "2024-06-01T00:00:00Z"
#end_date_usgs = "2024-06-10T23:59:59Z"
#
#def fetch_event_items(collection, start_date, end_date):
#    items = []
#    url = (
#        f"https://montandon-eoapi-stage.ifrc.org/stac/collections/{collection}/items"
#        f"?limit=200&datetime={start_date}/{end_date}"
#    )
#    #headers = {"Authorization": f"Bearer {token}"}
#    while url:
#        resp = requests.get(url) #headers=headers)
#        if resp.status_code != 200:
#            print(f"Error {resp.status_code} on {collection}: {resp.text}")
#            break
#        data = resp.json()
#        features = data.get("features", [])
#        # Only features with 'event' in roles
#        items.extend(
#            [f for f in features if "event" in f.get("properties", {}).get("roles", [])]
#        )
#        url = next((l.get("href") for l in data.get("links", []) if l.get("rel") == "next"), None)
#    return items
#
## Full year for glide and gdacs events
#for collection in collections_with_full_year:
#    print(f"Fetching events from {collection} (year)...")
#    events = fetch_event_items(collection, start_date_full, end_date_full)
#    print(f"  Found {len(events)} event items in {collection}")
#    filename = f"{collection}_events_2024.json"
#    with open(filename, "w", encoding="utf-8") as f:
#        json.dump(events, f, indent=2)
#
## One month for usgs events
#print(f"Fetching events from {usgs_collection} (June 2024)...")
#usgs_events = fetch_event_items(usgs_collection, start_date_usgs, end_date_usgs)
#print(f"  Found {len(usgs_events)} event items in {usgs_collection}")
#with open("usgs_events_2024_06.json", "w", encoding="utf-8") as f:
#    json.dump(usgs_events, f, indent=2)


from pystac_client import Client
from datetime import datetime
import json


STAC_API_URL = "https://montandon-eoapi-stage.ifrc.org/stac"

# Initialize client with authentication
#headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
client = Client.open(STAC_API_URL) #headers=headers)

# Define collections and date ranges
collections_config = {
    "glide-events": ("2024-01-01", "2024-06-30"),
    "gdacs-events": ("2024-01-01", "2024-06-30"),
    "usgs-events": ("2024-06-01", "2024-06-10")
}

for collection_id, (start, end) in collections_config.items():
    print(f"Fetching {collection_id}...")
    
    # Search with automatic pagination
    search = client.search(
        collections=[collection_id],
        datetime=f"{start}/{end}",
        max_items=None  # Gets all items
    )
    
    # Filter items with 'event' role
    events = [
        item.to_dict() 
        for item in search.items()
        if "event" in item.properties.get("roles", [])
    ]
    
    print(f"  Found {len(events)} event items")