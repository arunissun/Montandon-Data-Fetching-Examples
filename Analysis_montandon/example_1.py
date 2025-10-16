"""
This script retrieves all disaster event items from the Montandon STAC API's 'usgs-events' collection,
fetching results across all pages using standard STAC pagination.

Features:
- Tracks and prints the number of events per page and cumulative total, using len(data["features"]).
- Requests only selected keys ('id', 'assets', 'properties') in each event's output.

Requires:
    - requests (pip install requests)
"""

import requests

# Define base API URL and endpoint for USGS events
base_url = "https://montandon-eoapi-stage.ifrc.org/stac"
endpoint = "/collections/usgs-events/items"

# Set your query parameters (adjust values as needed)
params = {
    "limit": 50,  # 50 items per page
    "bbox": "-125,24,-66,50",  # Example bounding box for USA
    "datetime": "2020-01-01T00:00:00Z/2020-02-01T00:00:00Z",  # Filter by year (2020)
    "sortby": "+datetime",
    "fields": "id,assets,properties"  # Only request specific keys per event
}

all_items = []  # Will hold all returned items
next_url = base_url + endpoint
page_num = 1  # Track pagination

while next_url:
    response = requests.get(next_url, params=params)
    if not response.ok:
        print(f"Request failed (Page {page_num}):", response.status_code, response.text)
        break

    data = response.json()
    page_count = len(data["features"])
    all_items.extend(data["features"])

    print(f"Page {page_num}: Discovered {page_count} events, Cumulative total: {len(all_items)}")

    # Find pagination 'next' link
    next_link = None
    for link in data.get("links", []):
        if link.get("rel") == "next":
            next_link = link.get("href")
            break

    if next_link:
        next_url = next_link
        params = {}
        page_num += 1
    else:
        next_url = None

print(f"\nFetched {len(all_items)} events across all pages.")
