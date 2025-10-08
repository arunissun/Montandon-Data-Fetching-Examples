


import requests
import json
from datetime import datetime, timedelta



search_url = "https://montandon-eoapi-stage.ifrc.org/stac/search"
#headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

# Date setup for "last month"
today = datetime.utcnow()
start = (today - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
end = today.strftime("%Y-%m-%dT%H:%M:%SZ")

collection = "usgs-events"
hazard_code = "nat-geo-ear-gro"  # Earthquake ground motion

body = {
    "collections": [collection],
    "query": {
        "roles": {"eq": "event"},
        "monty:hazard_codes": {"eq": hazard_code},
        "datetime": {"gte": start, "lte": end}
    },
    "limit": 100
}

response = requests.post(search_url, json=body)
print("Status Code:", response.status_code)
data = response.json()
print(json.dumps(data, indent=2))
items = data.get("features", [])
print(f"Found {len(items)} matching event items")

for i, item in enumerate(items):
    print(
        f"{i+1}: ID={item.get('id')}, Date={item['properties'].get('datetime')}, "
        f"HazardCodes={item['properties'].get('monty:hazard_codes')}, "
        f"Roles={item['properties'].get('roles')}"
    )
