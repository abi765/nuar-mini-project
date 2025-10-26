import requests
import json

# Overpass API endpoint
url = "https://overpass-api.de/api/interpreter"

# Overpass QL query
query = """
[out:json];
area[name="Manchester"]->.a;
node["amenity"="pub"](area.a);
out;
"""

# Send the request
response = requests.get(url, params={'data': query})
data = response.json()

# Print first few results
for element in data['elements'][:5]:
    print(element['tags'].get('name', 'Unnamed Pub'), element['lat'], element['lon'])