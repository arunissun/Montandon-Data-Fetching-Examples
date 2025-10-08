
import requests
import json 



url = "https://montandon-eoapi-stage.ifrc.org/stac/collections/usgs-events/queryables"
resp = requests.get(url) # headers={"Authorization": f"Bearer {BEARER_TOKEN}"})
data = resp.json()
# Pretty print the JSON
print(json.dumps(data, indent=2))
