"""
API Client Utilities for NUAR Mini Project
Reusable functions for calling all 4 APIs
"""

import requests
import time
import json
from typing import Dict, List, Optional, Any
from datetime import datetime


class OverpassClient:
    """Client for Overpass API (OpenStreetMap data)"""
    
    def __init__(self, timeout: int = 300):
        self.endpoints = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.openstreetmap.ru/api/interpreter"
        ]
        self.timeout = timeout
        self.current_endpoint = 0
    
    def query_infrastructure(self, bbox: Dict, infrastructure_types: List[str]) -> Optional[Dict]:
        """
        Query Overpass API for infrastructure elements
        
        Args:
            bbox: Bounding box dict with south, west, north, east
            infrastructure_types: List of infrastructure types to query
        
        Returns:
            API response as dict, or None if failed
        """
        # Build query for multiple infrastructure types
        queries = []
        
        for infra_type in infrastructure_types:
            if infra_type == "manhole":
                queries.append(f'node["man_made"="manhole"]({bbox["south"]},{bbox["west"]},{bbox["north"]},{bbox["east"]})')
            elif infra_type == "pipeline":
                queries.append(f'way["man_made"="pipeline"]({bbox["south"]},{bbox["west"]},{bbox["north"]},{bbox["east"]})')
            elif infra_type == "power_cable":
                queries.append(f'way["power"="cable"]({bbox["south"]},{bbox["west"]},{bbox["north"]},{bbox["east"]})')
            elif infra_type == "cable":
                queries.append(f'way["man_made"="cable"]({bbox["south"]},{bbox["west"]},{bbox["north"]},{bbox["east"]})')
            elif infra_type == "duct":
                queries.append(f'way["man_made"="duct"]({bbox["south"]},{bbox["west"]},{bbox["north"]},{bbox["east"]})')
            elif infra_type == "utility_pole":
                queries.append(f'node["power"="pole"]({bbox["south"]},{bbox["west"]},{bbox["north"]},{bbox["east"]})')
        
        # Construct full query
        query_str = ";\n  ".join(queries)
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          {query_str};
        );
        out body geom;
        """
        
        print(f"🔍 Querying Overpass API for {len(infrastructure_types)} infrastructure types...")
        print(f"   Bounding box: {bbox}")
        print(f"   Timeout: {self.timeout}s")
        
        # Try endpoints with fallback
        for attempt in range(3):
            try:
                endpoint = self.endpoints[self.current_endpoint]
                print(f"   Attempt {attempt + 1}: Using {endpoint}")
                
                response = requests.post(
                    endpoint,
                    data={'data': query},
                    timeout=self.timeout + 10
                )
                response.raise_for_status()
                result = response.json()
                
                elements_count = len(result.get('elements', []))
                print(f"   ✅ SUCCESS: Retrieved {elements_count} elements")
                
                return result
                
            except requests.exceptions.Timeout:
                print(f"   ⏱️  Timeout on attempt {attempt + 1}")
                self.current_endpoint = (self.current_endpoint + 1) % len(self.endpoints)
                time.sleep(5)
                
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Error on attempt {attempt + 1}: {e}")
                self.current_endpoint = (self.current_endpoint + 1) % len(self.endpoints)
                time.sleep(5)
        
        print("   ❌ FAILED: All attempts exhausted")
        return None


class PoliceAPIClient:
    """Client for UK Police Data API"""
    
    def __init__(self):
        self.base_url = "https://data.police.uk/api"
    
    def query_crimes_at_location(self, lat: float, lon: float, date: Optional[str] = None) -> List[Dict]:
        """
        Query crimes at a specific location
        
        Args:
            lat: Latitude
            lon: Longitude
            date: Optional date in YYYY-MM format
        
        Returns:
            List of crime records
        """
        url = f"{self.base_url}/crimes-street/all-crime"
        params = {'lat': lat, 'lng': lon}
        
        if date:
            params['date'] = date
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            crimes = response.json()
            return crimes if isinstance(crimes, list) else []
            
        except Exception as e:
            print(f"   ❌ Police API error: {e}")
            return []
    
    def query_crimes_grid(self, bbox: Dict, grid_size: int = 3) -> List[Dict]:
        """
        Query crimes using a grid pattern to cover entire area
        
        Args:
            bbox: Bounding box with south, west, north, east
            grid_size: Number of grid points per dimension (3 = 9 points)
        
        Returns:
            Combined list of all crime records
        """
        print(f"🔍 Querying Police API with {grid_size}x{grid_size} grid...")
        
        all_crimes = []
        seen_ids = set()
        
        # Create grid of points
        lat_step = (bbox['north'] - bbox['south']) / grid_size
        lon_step = (bbox['east'] - bbox['west']) / grid_size
        
        for i in range(grid_size):
            for j in range(grid_size):
                lat = bbox['south'] + (i + 0.5) * lat_step
                lon = bbox['west'] + (j + 0.5) * lon_step
                
                print(f"   Grid point ({i+1},{j+1}): ({lat:.4f}, {lon:.4f})")
                
                crimes = self.query_crimes_at_location(lat, lon)
                
                # Deduplicate by crime ID
                for crime in crimes:
                    crime_id = crime.get('id') or crime.get('persistent_id')
                    if crime_id and crime_id not in seen_ids:
                        seen_ids.add(crime_id)
                        all_crimes.append(crime)
                
                time.sleep(1)  # Rate limiting
        
        print(f"   ✅ Total unique crimes: {len(all_crimes)}")
        return all_crimes


class WeatherAPIClient:
    """Client for OpenWeatherMap API"""
    
    def __init__(self, api_key: str):
        self.base_url = "https://api.openweathermap.org/data/2.5"
        self.api_key = api_key
    
    def get_current_weather(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Get current weather for a location
        
        Args:
            lat: Latitude
            lon: Longitude
        
        Returns:
            Weather data dict or None
        """
        url = f"{self.base_url}/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            print(f"   ❌ Weather API error: {e}")
            return None


class PostcodesAPIClient:
    """Client for Postcodes.io API"""
    
    def __init__(self):
        self.base_url = "https://api.postcodes.io"
    
    def lookup_postcode(self, postcode: str) -> Optional[Dict]:
        """
        Lookup a single postcode
        
        Args:
            postcode: UK postcode (spaces optional)
        
        Returns:
            Postcode data dict or None
        """
        postcode_clean = postcode.replace(' ', '')
        url = f"{self.base_url}/postcodes/{postcode_clean}"
        
        try:
            response = requests.get(url, timeout=30)
            data = response.json()
            
            if data.get('status') == 200:
                return data.get('result')
            return None
            
        except Exception as e:
            print(f"   ❌ Postcodes API error for {postcode}: {e}")
            return None
    
    def lookup_postcodes_bulk(self, postcodes: List[str]) -> List[Dict]:
        """
        Lookup multiple postcodes in bulk (up to 100)
        
        Args:
            postcodes: List of UK postcodes
        
        Returns:
            List of postcode data dicts
        """
        url = f"{self.base_url}/postcodes"
        
        # API allows max 100 postcodes per request
        results = []
        
        for i in range(0, len(postcodes), 100):
            batch = postcodes[i:i+100]
            
            try:
                response = requests.post(
                    url,
                    json={'postcodes': batch},
                    timeout=30
                )
                data = response.json()
                
                if data.get('status') == 200:
                    for item in data.get('result', []):
                        if item.get('result'):
                            results.append(item['result'])
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"   ❌ Bulk lookup error: {e}")
        
        return results
    
    def search_postcodes_in_area(self, outward_codes: List[str]) -> List[Dict]:
        """
        Search for all postcodes in given outward code areas (e.g., SK1, SK2)
        
        Args:
            outward_codes: List of outward codes (e.g., ['SK1', 'SK2'])
        
        Returns:
            List of all postcodes found
        """
        print(f"🔍 Searching postcodes in {len(outward_codes)} outward code areas...")
        
        all_postcodes = []
        
        for outward in outward_codes:
            url = f"{self.base_url}/outcodes/{outward}"
            
            try:
                response = requests.get(url, timeout=30)
                data = response.json()
                
                if data.get('status') == 200:
                    result = data.get('result')
                    if result:
                        all_postcodes.append(result)
                        print(f"   ✅ {outward}: Found data")
                    else:
                        print(f"   ⚠️  {outward}: No data")
                else:
                    print(f"   ❌ {outward}: Failed")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"   ❌ Error for {outward}: {e}")
        
        print(f"   ✅ Total outward codes retrieved: {len(all_postcodes)}")
        return all_postcodes


# Helper function to add metadata
def add_metadata(data: Any, source: str, area: str) -> Dict:
    """
    Add metadata fields to raw API data
    
    Args:
        data: Raw API response data
        source: Source API name
        area: Geographic area name
    
    Returns:
        Dict with data and metadata
    """
    return {
        'raw_data': data,
        'metadata': {
            'ingestion_timestamp': datetime.now().isoformat(),
            'source_api': source,
            'area': area,
            'ingestion_date': datetime.now().strftime('%Y-%m-%d')
        }
    }