"""
Test All 4 APIs for NUAR Mini Project
Loads API keys from .env file
"""

import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

print("="*70)
print("🧪 Testing All 4 APIs for NUAR Mini Project")
print("="*70)
print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Test configuration - Manchester Centre
TEST_REGION = {
    "name": "Manchester City Centre",
    "lat": 53.48,
    "lon": -2.235,
    "bbox": {
        "south": 53.46,
        "west": -2.26,
        "north": 53.50,
        "east": -2.21
    },
    "postcode": "SK8 3AU"
}

print(f"📍 Test Region: {TEST_REGION['name']}")
print(f"   Coordinates: ({TEST_REGION['lat']}, {TEST_REGION['lon']})")
print()

# Results tracking
results = {}

# ============================================================================
# 1. OVERPASS API - Infrastructure Data
# ============================================================================
def test_overpass():
    """Test Overpass API - Infrastructure data (pipelines, manholes, cables)"""
    print("1️⃣  Testing Overpass API (OpenStreetMap Infrastructure)...")
    
    endpoint = "https://overpass-api.de/api/interpreter"
    bbox = TEST_REGION['bbox']
    
    # Query for manholes in Manchester Centre
    query = f"""
    [out:json][timeout:25];
    (
      node["man_made"="manhole"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
    );
    out geom;
    """
    
    try:
        response = requests.post(
            endpoint, 
            data={'data': query}, 
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        
        count = len(result.get('elements', []))
        timestamp = result.get('osm3s', {}).get('timestamp_osm_base', 'N/A')
        
        print(f"   ✅ SUCCESS: Found {count} manholes")
        print(f"   📊 OSM Data Timestamp: {timestamp}")
        
        if count > 0:
            sample = result['elements'][0]
            print(f"   📋 Sample: ID={sample.get('id')}, Type={sample.get('type')}")
        
        results['overpass'] = {'status': 'success', 'count': count}
        return result
        
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['overpass'] = {'status': 'failed', 'error': str(e)}
        return None

# ============================================================================
# 2. UK POLICE API - Crime Data
# ============================================================================
def test_police_api():
    """Test UK Police API - Street-level crime data"""
    print("\n2️⃣  Testing UK Police API (Crime Data)...")
    
    url = "https://data.police.uk/api/crimes-street/all-crime"
    params = {
        'lat': TEST_REGION['lat'],
        'lng': TEST_REGION['lon']
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        crimes = response.json()
        
        count = len(crimes)
        print(f"   ✅ SUCCESS: Found {count} crime records")
        
        if count > 0:
            sample = crimes[0]
            category = sample.get('category', 'N/A')
            month = sample.get('month', 'N/A')
            location = sample.get('location', {}).get('street', {}).get('name', 'N/A')
            
            print(f"   📋 Sample: {category} on {month} near {location}")
            
            # Count by category
            categories = {}
            for crime in crimes[:10]:  # First 10 for summary
                cat = crime.get('category', 'unknown')
                categories[cat] = categories.get(cat, 0) + 1
            
            print(f"   📊 Top Categories: {dict(list(categories.items())[:3])}")
        
        results['police'] = {'status': 'success', 'count': count}
        return crimes
        
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['police'] = {'status': 'failed', 'error': str(e)}
        return None

# ============================================================================
# 3. OPENWEATHERMAP API - Weather Data
# ============================================================================
def test_weather_api():
    """Test OpenWeatherMap API - Current weather data"""
    print("\n3️⃣  Testing OpenWeatherMap API (Weather Data)...")
    
    # Load API key from environment
    api_key = os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        print("   ⚠️  SKIPPED: No API key found in .env file")
        print("   💡 Add OPENWEATHER_API_KEY to your .env file")
        results['weather'] = {'status': 'skipped', 'reason': 'no_api_key'}
        return None
    
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': TEST_REGION['lat'],
        'lon': TEST_REGION['lon'],
        'appid': api_key,
        'units': 'metric'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract weather info
        temp = data.get('main', {}).get('temp', 'N/A')
        feels_like = data.get('main', {}).get('feels_like', 'N/A')
        humidity = data.get('main', {}).get('humidity', 'N/A')
        weather = data.get('weather', [{}])[0].get('description', 'N/A')
        wind_speed = data.get('wind', {}).get('speed', 'N/A')
        
        print(f"   ✅ SUCCESS: Weather data retrieved")
        print(f"   🌡️  Temperature: {temp}°C (feels like {feels_like}°C)")
        print(f"   ☁️  Conditions: {weather}")
        print(f"   💧 Humidity: {humidity}%")
        print(f"   💨 Wind Speed: {wind_speed} m/s")
        
        results['weather'] = {'status': 'success', 'temp': temp}
        return data
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f"   ❌ FAILED: Invalid API key")
            print(f"   💡 Check your OPENWEATHER_API_KEY in .env file")
        else:
            print(f"   ❌ FAILED: {e}")
        results['weather'] = {'status': 'failed', 'error': str(e)}
        return None
        
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['weather'] = {'status': 'failed', 'error': str(e)}
        return None

# ============================================================================
# 4. POSTCODES.IO API - Postcode Lookup
# ============================================================================
def test_postcodes_api():
    """Test Postcodes.io API - UK postcode geocoding"""
    print("\n4️⃣  Testing Postcodes.io API (Geo Lookup)...")
    
    # Remove spaces from postcode for URL
    postcode = TEST_REGION['postcode'].replace(' ', '')
    url = f"https://api.postcodes.io/postcodes/{postcode}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') == 200:
            result = data.get('result', {})
            postcode_full = result.get('postcode', 'N/A')
            admin_district = result.get('admin_district', 'N/A')
            region = result.get('region', 'N/A')
            country = result.get('country', 'N/A')
            lat = result.get('latitude', 'N/A')
            lon = result.get('longitude', 'N/A')
            
            print(f"   ✅ SUCCESS: Postcode data retrieved")
            print(f"   📮 Postcode: {postcode_full}")
            print(f"   🏛️  District: {admin_district}, {region}")
            print(f"   🌍 Country: {country}")
            print(f"   📍 Coordinates: ({lat}, {lon})")
            
            results['postcodes'] = {'status': 'success', 'postcode': postcode_full}
            return result
        else:
            print(f"   ❌ FAILED: Invalid response")
            results['postcodes'] = {'status': 'failed', 'error': 'invalid_response'}
            return None
            
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['postcodes'] = {'status': 'failed', 'error': str(e)}
        return None

# ============================================================================
# RUN ALL TESTS
# ============================================================================

overpass_result = test_overpass()
police_result = test_police_api()
weather_result = test_weather_api()
postcodes_result = test_postcodes_api()

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*70)
print("📊 TEST SUMMARY")
print("="*70)

total_tests = len(results)
successful = sum(1 for r in results.values() if r['status'] == 'success')
failed = sum(1 for r in results.values() if r['status'] == 'failed')
skipped = sum(1 for r in results.values() if r['status'] == 'skipped')

print(f"\n✅ Successful: {successful}/{total_tests}")
print(f"❌ Failed: {failed}/{total_tests}")
print(f"⚠️  Skipped: {skipped}/{total_tests}")

print("\nAPI Status:")
for api_name, result in results.items():
    status_icon = {
        'success': '✅',
        'failed': '❌',
        'skipped': '⚠️'
    }.get(result['status'], '❓')
    
    print(f"  {status_icon} {api_name.upper()}: {result['status']}")
    if 'count' in result:
        print(f"      Records: {result['count']}")
    if 'error' in result:
        print(f"      Error: {result['error']}")

# ============================================================================
# NEXT STEPS
# ============================================================================

print("\n" + "="*70)
print("🚀 NEXT STEPS")
print("="*70)

if successful == total_tests:
    print("\n🎉 All APIs working perfectly!")
    print("\n✅ You're ready to:")
    print("   1. Initialize Git repository")
    print("   2. Push to GitHub")
    print("   3. Connect Databricks Repos")
    print("   4. Start building Bronze layer")
    
elif successful >= 3:
    print("\n✨ Most APIs working! You can proceed.")
    print("\n💡 Optional fixes:")
    if results.get('weather', {}).get('status') == 'skipped':
        print("   - Add OPENWEATHER_API_KEY to .env file")
    
elif successful >= 2:
    print("\n⚠️  Some APIs need attention before proceeding.")
    print("\n🔧 Please fix:")
    for api_name, result in results.items():
        if result['status'] == 'failed':
            print(f"   - {api_name.upper()}: {result.get('error', 'Unknown error')}")
    
else:
    print("\n❌ Too many API failures. Please check:")
    print("   - Internet connection")
    print("   - API endpoints are accessible")
    print("   - .env file is properly configured")

print("\n" + "="*70)

# Save results to file
with open('data/sample/api_test_results.json', 'w') as f:
    json.dump({
        'timestamp': datetime.now().isoformat(),
        'test_region': TEST_REGION,
        'results': results
    }, f, indent=2)

print("\n💾 Results saved to: data/sample/api_test_results.json")