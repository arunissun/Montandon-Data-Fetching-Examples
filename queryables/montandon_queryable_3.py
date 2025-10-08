


#import requests
#import json
#import urllib.parse
#
#
#base_url = "https://montandon-eoapi-stage.ifrc.org/stac/search"
#headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
#
#collections = [
#    "desinventar-events", "desinventar-impacts", "emdat-events", "emdat-hazards", "emdat-impacts",
#    "gdacs-events", "gdacs-hazards", "gdacs-impacts",
#    "gfd-events", "gfd-hazards", "gfd-impacts",
#    "glide-events", "glide-hazards", "ibtracs-events", "ibtracs-hazards",
#    "idmc-gidd-events", "idmc-gidd-impacts", "idmc-idu-events", "idmc-idu-impacts",
#    "ifrcevent-events", "ifrcevent-hazards", "ifrcevent-impacts",
#    "pdc-events", "pdc-hazards", "pdc-impacts",
#    "reference-events",
#    "usgs-events", "usgs-hazards", "usgs-impacts"
#]
#
## Set a sample date window (last 1 month)
#from datetime import datetime, timedelta
#today = datetime.utcnow()
#start_date = (today - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
#end_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")
#
## Standard CQL2-JSON filter: just search for items with 'event' in roles and the date range.
#cql2_filter_json = {
#    "op": "and",
#    "args": [
#        { "op": "in", "args": ["event", { "property": "roles" }] },
#        { "op": ">=", "args": [ {"property": "datetime"}, start_date ] },
#        { "op": "<=", "args": [ {"property": "datetime"}, end_date ] }
#    ]
#}
#
#filter_str = json.dumps(cql2_filter_json)
#filter_str_encoded = urllib.parse.quote(filter_str)
#
#for collection in collections:
#    print(f"Trying collection: {collection}")
#    url = (
#        f"{base_url}"
#        f"?collections={collection}"
#        f"&filter={filter_str_encoded}"
#        f"&filter-lang=cql2-json"
#        f"&limit=10"
#    )
#    response = requests.get(url) #headers=headers)
#    print("  Status:", response.status_code)
#    if response.status_code == 200:
#        data = response.json()
#        items = data.get("features", [])
#        print(f"  Found {len(items)} matching records")
#        # Optionally preview first record
#        if items:
#            print(json.dumps(items[0], indent=2))
#    else:
#        print("  Response content:", response.text)


from pystac_client import Client
from datetime import datetime, timedelta
import json


STAC_API_URL = "https://montandon-eoapi-stage.ifrc.org/stac"

# Initialize client with authentication
#headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
client = Client.open(STAC_API_URL) # headers=headers)

# Define collections
collections = [
    "desinventar-events", "desinventar-impacts", "emdat-events", "emdat-hazards", "emdat-impacts",
    "gdacs-events", "gdacs-hazards", "gdacs-impacts",
    "gfd-events", "gfd-hazards", "gfd-impacts",
    "glide-events", "glide-hazards", "ibtracs-events", "ibtracs-hazards",
    "idmc-gidd-events", "idmc-gidd-impacts", "idmc-idu-events", "idmc-idu-impacts",
    "ifrcevent-events", "ifrcevent-hazards", "ifrcevent-impacts",
    "pdc-events", "pdc-hazards", "pdc-impacts",
    "reference-events",
    "usgs-events", "usgs-hazards", "usgs-impacts"
]

# Date range (last 180 days)
today = datetime.utcnow()
start_date = (today - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
end_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")

# CQL2 filter for items with 'event' role
cql2_filter = {
    "op": "and",
    "args": [
        {"op": "in", "args": ["event", {"property": "roles"}]},
        {"op": ">=", "args": [{"property": "datetime"}, start_date]},
        {"op": "<=", "args": [{"property": "datetime"}, end_date]}
    ]
}

# Query each collection
for collection in collections:
    print(f"Querying collection: {collection}")
    
    try:
        # Search with CQL2 filter
        search = client.search(
            collections=[collection],
            filter=cql2_filter,
            filter_lang="cql2-json",
            max_items=10  # Limit to 10 items per collection
        )
        
        # Get items
        items = list(search.items())
        print(f"  Found {len(items)} matching records")
        
        # Preview first item
        if items:
            first_item = items[0].to_dict()
            print(json.dumps(first_item, indent=2))
            
    except Exception as e:
        print(f"  Error: {str(e)}")
