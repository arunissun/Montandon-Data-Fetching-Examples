

import requests
import json

flood_collections = ["glide-hazards", "gdacs-hazards"]
earthquake_collections = ["usgs-hazards"]

# All Montandon flood hazard codes
flood_hazard_codes = [
    "nat-hyd-flo-riv", "nat-hyd-flo-coa", "nat-hyd-flo-flo",
    "nat-hyd-flo-ice", "nat-cli-glo-glo", "tec-mis-col-col", "nat-hyd-flo-fla"
]
# All earthquake hazard codes
earthquake_hazard_codes = ["nat-geo-ear-gro"]

# 1 year for floods
flood_start = "2024-01-01T00:00:00Z"
flood_end = "2024-12-31T23:59:59Z"
# 1 week for earthquakes (example: June 1-7, 2024)
eq_start = "2024-06-01T00:00:00Z"
eq_end = "2024-08-07T23:59:59Z"

def fetch_all_hazards(collection, start_date, end_date):
    hazards = []
    url = (
        f"https://montandon-eoapi-stage.ifrc.org/stac/collections/{collection}/items"
        f"?limit=200&datetime={start_date}/{end_date}"
    )
    #headers = {"Authorization": f"Bearer {token}"}
    while url:
        resp = requests.get(url) # headers=headers)
        if resp.status_code != 200:
            print(f"Error {resp.status_code} on {collection}: {resp.text}")
            break
        data = resp.json()
        features = data.get("features", [])
        hazards.extend(
            [f for f in features if "hazard" in f.get("properties", {}).get("roles", [])]
        )
        url = next((l.get("href") for l in data.get("links", []) if l.get("rel") == "next"), None)
    return hazards

# --- Floods: 1 year ---
all_floods = []
for coll in flood_collections:
    print(f"Fetching floods from {coll}...")
    items = fetch_all_hazards(coll, flood_start, flood_end)
    floods = [
        f for f in items
        if any(code in flood_hazard_codes for code in f.get("properties", {}).get("monty:hazard_codes", []))
    ]
    print(f"  Found {len(floods)} flood hazard items in {coll}")
    all_floods.extend(floods)

# --- Earthquakes: 1 week ---
all_eqs = []
for coll in earthquake_collections:
    print(f"Fetching earthquakes from {coll}...")
    items = fetch_all_hazards(coll, eq_start, eq_end)
    eqs = [
        f for f in items
        if any(code in earthquake_hazard_codes for code in f.get("properties", {}).get("monty:hazard_codes", []))
    ]
    print(f"  Found {len(eqs)} earthquake hazard items in {coll}")
    all_eqs.extend(eqs)

# Optionally save results
with open("all_flood_hazards.json", "w", encoding="utf-8") as f:
    json.dump(all_floods, f, indent=2)
with open("all_earthquake_hazards.json", "w", encoding="utf-8") as f:
    json.dump(all_eqs, f, indent=2)

# Print all unique country and hazard codes found (for inspection)
def print_unique(items, label):
    countries = set()
    hazards = set()
    for item in items:
        countries.update(item["properties"].get("monty:country_codes", []))
        hazards.update(item["properties"].get("monty:hazard_codes", []))
    print(f"{label}: Unique countries: {sorted(countries)}")
    print(f"{label}: Unique hazard codes: {sorted(hazards)}")

print_unique(all_floods, "Floods")
print_unique(all_eqs, "Earthquakes")
