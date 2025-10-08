
from pystac_client import Client
from datetime import datetime, timedelta



STAC_API_URL = "https://montandon-eoapi-stage.ifrc.org/stac"

# Use only collections ending with "-events"
collections = [
    "desinventar-events", "emdat-events", "gdacs-events", "gfd-events", 
    "glide-events", "ibtracs-events", "idmc-gidd-events", "idmc-idu-events",
    "ifrcevent-events", "pdc-events", "reference-events", "usgs-events"
]

# Europe bounding box for spatial filtering as a Polygon
EUROPE_POLYGON = {
    "type": "Polygon",
    "coordinates": [[
        [-31.266, 34.5], [39.869, 34.5],
        [39.869, 71.185], [-31.266, 71.185],
        [-31.266, 34.5]  # closing ring
    ]]
}

today = datetime.utcnow()
start_date = (today - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
end_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")

# Use queryables for property names! We filter for: roles='event', spatial (geometry), temporal (datetime)
cql2_filter = {
    "op": "and",
    "args": [
        {"op": "s_intersects", "args": [
            {"property": "geometry"}, EUROPE_POLYGON
        ]},
        {"op": "=", "args": [
            {"property": "roles"}, "event"
        ]},
        {"op": "t_during", "args": [
            {"property": "datetime"}, {"interval": [start_date, end_date]}
        ]}
    ]
}

#headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
client = Client.open(STAC_API_URL)

for collection in collections:
    print(f"\nSearching in collection: {collection}")
    try:
        search = client.search(
            collections=[collection],
            filter=cql2_filter,
            filter_lang="cql2-json",
            limit=100  # items per page, will paginate if more exist
        )
        items = list(search.get_all_items())
        print(f"  Events found in Europe in last 2 months: {len(items)}")
        # Optionally show some details for the first event (if any found):
        if items:
            first = items[0]
            props = first.properties
            print(f"    Sample: ID={first.id}, Date={props.get('datetime')}, Roles={props.get('roles')}")
    except Exception as e:
        print(f"  Error searching {collection}: {e}")
