
from pystac_client import Client
from datetime import datetime, timedelta





STAC_API_URL = "https://montandon-eoapi-stage.ifrc.org/stac"




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


EUROPE_POLYGON = {
    "type": "Polygon",
    "coordinates": [[
        [-31.266, 34.5], [39.869, 34.5],
        [39.869, 71.185], [-31.266, 71.185],
        [-31.266, 34.5]
    ]]
}

today = datetime.utcnow()
start_date = (today - timedelta(days=600)).strftime("%Y-%m-%dT%H:%M:%SZ")
end_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")

cql2_advanced = {
    "op": "and",
    "args": [
        {"op": "s_intersects", "args": [
            {"property": "geometry"},
            EUROPE_POLYGON
        ]},
        {"op": "in", "args": [
            "event", {"property": "roles"}
        ]},
        {"op": "t_during", "args": [
            {"property": "datetime"},
            {"interval": [start_date, end_date]}
        ]}
    ]
}

#headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
client = Client.open(STAC_API_URL) # headers=headers)

for collection in collections:
    print(f"Searching in collection: {collection}")
    try:
        search = client.search(
            collections=[collection],
            filter=cql2_advanced,
            filter_lang="cql2-json",
            limit=100  # Batch size per page; you may adjust (docs say up to 10,000)
        )
        items = list(search.get_all_items())  # Collects ALL paginated results from API!
        print(f"  Events found in Europe in last 2 months: {len(items)}")
    except Exception as e:
        print(f"  Error searching {collection}: {e}")
