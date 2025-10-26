# test_all_apis_local.py

import requests
import json
from datetime import datetime

print("="*60)
print("Testing All 4 APIs for NUAR Mini Project")
print("="*60)

# 1. Overpass API - Infrastructure (YOU ALREADY HAVE THIS WORKING!)
def test_overpass():
    """Test infrastructure data"""
    endpoint = "https://overpass-api.de/api/interpreter"
    # Test manholes in Manchester Centre
    query = """
    [out:json][timeout:25];
    (node["man_made"="manhole"](53.46,-2.26,53.50,-2.21););
    out geom;
    """
    response = requests.post(endpoint, data={'data': query}, timeout=30)
    result = response.json()
    count = len(result.get('elements', []))
    print(f"✅ Overpass API: {count} manholes found in Manchester Centre")
    return result

# 2. UK Police API - Crime Data
def test_police_api():
    """Test crime data"""
    url = "https://data.police.uk/api/crimes-street/all-crime"
    params = {'lat': 53.48, 'lng': -2.235}  # Manchester Centre
    
    response = requests.get(url, params=params, timeout=30)
    crimes = response.json()
    count = len(crimes)
    
    if count > 0:
        print(f"✅ Police API: {count} crimes found near Manchester Centre")
        print(f"   Sample: {crimes[0].get('category', 'N/A')} on {crimes[0].get('month', 'N/A')}")
    else:
        print(f"⚠️  Police API: No crimes returned (might be rate limited, try again)")
    return crimes

# 3. OpenWeatherMap API
def test_weather_api():
    """Test weather data - REQUIRES API KEY"""
    api_key = "YOUR_API_KEY_HERE"  # Replace with your key from openweathermap.org
    
    if api_key == "YOUR_API_KEY_HERE":
        print("⚠️  Weather API: Skipped (need API key from openweathermap.org)")
        return None
    
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': 53.48,
        'lon': -2.235,
        'appid': api_key,
        'units': 'metric'
    }
    
    response = requests.get(url, params=params, timeout=30)
    data = response.json()
    
    if response.status_code == 200:
        temp = data.get('main', {}).get('temp', 'N/A')
        weather = data.get('weather', [{}])[0].get('description', 'N/A')
        print(f"✅ Weather API: Manchester is {temp}°C, {weather}")
    else:
        print(f"❌ Weather API: {data.get('message', 'Error')}")
    return data

# 4. Postcodes.io API
def test_postcodes_api():
    """Test postcode lookup"""
    # Test with Manchester postcode
    url = "https://api.postcodes.io/postcodes/M1 1AA"
    
    response = requests.get(url, timeout=30)
    data = response.json()
    
    if data.get('status') == 200:
        result = data.get('result', {})
        postcode = result.get('postcode', 'N/A')
        admin_district = result.get('admin_district', 'N/A')
        lat, lon = result.get('latitude'), result.get('longitude')
        print(f"✅ Postcodes API: {postcode} is in {admin_district}")
        print(f"   Coordinates: ({lat}, {lon})")
    else:
        print(f"❌ Postcodes API: Failed")
    return data

# Run all tests
print("\n1. Testing Overpass API (Infrastructure)...")
overpass_result = test_overpass()

print("\n2. Testing UK Police API (Crime Data)...")
police_result = test_police_api()

print("\n3. Testing OpenWeatherMap API (Weather)...")
weather_result = test_weather_api()

print("\n4. Testing Postcodes.io API (Geo Lookup)...")
postcodes_result = test_postcodes_api()

print("\n" + "="*60)
print("API Testing Complete!")
print("="*60)
print("\nNext: Sign up for OpenWeatherMap API key (free)")
print("Then: Move to Databricks and start Bronze layer!")