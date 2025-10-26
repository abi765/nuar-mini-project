"""
Bronze Layer Ingestion Utilities
Functions for saving raw API data to Bronze layer
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd


def save_to_bronze_parquet(
    data: List[Dict],
    output_dir: str,
    filename: str,
    partition_cols: List[str] = None
) -> str:
    """
    Save data to Bronze layer as Parquet file
    
    Args:
        data: List of data records
        output_dir: Bronze layer directory
        filename: Output filename (without extension)
        partition_cols: Optional list of columns to partition by
    
    Returns:
        Path to saved file
    """
    if not data:
        print("   ⚠️  No data to save")
        return None
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Add ingestion metadata if not present
    if 'ingestion_timestamp' not in df.columns:
        df['ingestion_timestamp'] = datetime.now().isoformat()
    if 'ingestion_date' not in df.columns:
        df['ingestion_date'] = datetime.now().strftime('%Y-%m-%d')
    
    # Construct output path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f"{filename}_{timestamp}.parquet")
    
    # Save as Parquet
    try:
        if partition_cols:
            # Create partitioned directory structure
            partition_dir = os.path.join(output_dir, filename)
            os.makedirs(partition_dir, exist_ok=True)
            
            df.to_parquet(
                partition_dir,
                partition_cols=partition_cols,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            print(f"   💾 Saved {len(df)} rows to: {partition_dir} (partitioned by {partition_cols})")
            return partition_dir
        else:
            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            print(f"   💾 Saved {len(df)} rows to: {output_path}")
            return output_path
            
    except Exception as e:
        print(f"   ❌ Error saving Parquet: {e}")
        return None


def save_to_bronze_json(
    data: Any,
    output_dir: str,
    filename: str
) -> str:
    """
    Save raw API response to Bronze layer as JSON
    
    Args:
        data: Raw API response (dict or list)
        output_dir: Bronze layer directory
        filename: Output filename (without extension)
    
    Returns:
        Path to saved file
    """
    if not data:
        print("   ⚠️  No data to save")
        return None
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Construct output path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f"{filename}_{timestamp}.json")
    
    # Save as JSON
    try:
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Calculate size
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"   💾 Saved raw JSON to: {output_path} ({size_mb:.2f} MB)")
        return output_path
        
    except Exception as e:
        print(f"   ❌ Error saving JSON: {e}")
        return None


def flatten_overpass_elements(elements: List[Dict]) -> List[Dict]:
    """
    Flatten Overpass API elements into tabular format
    
    Args:
        elements: List of OSM elements from Overpass
    
    Returns:
        List of flattened records
    """
    flattened = []
    
    for elem in elements:
        tags = elem.get('tags', {})
        
        # Base record
        record = {
            'id': elem.get('id'),
            'type': elem.get('type'),
            'lat': elem.get('lat'),
            'lon': elem.get('lon'),
            
            # Infrastructure classification
            'infrastructure_type': (
                tags.get('man_made') or 
                tags.get('power') or 
                'unknown'
            ),
            
            # Common tags
            'substance': tags.get('substance'),
            'location': tags.get('location'),
            'operator': tags.get('operator'),
            'name': tags.get('name'),
            'diameter': tags.get('diameter'),
            'material': tags.get('material'),
            
            # All tags as JSON string
            'all_tags': json.dumps(tags)
        }
        
        # For ways, add geometry info
        if elem.get('type') == 'way':
            nodes = elem.get('nodes', [])
            geometry = elem.get('geometry', [])
            
            record['node_count'] = len(nodes)
            record['geometry_points'] = len(geometry)
            
            if geometry:
                record['first_lat'] = geometry[0].get('lat')
                record['first_lon'] = geometry[0].get('lon')
                record['last_lat'] = geometry[-1].get('lat')
                record['last_lon'] = geometry[-1].get('lon')
                
                # Store full geometry as JSON
                record['geometry_json'] = json.dumps(geometry)
        
        flattened.append(record)
    
    return flattened


def flatten_crime_records(crimes: List[Dict]) -> List[Dict]:
    """
    Flatten Police API crime records into tabular format
    
    Args:
        crimes: List of crime records from Police API
    
    Returns:
        List of flattened records
    """
    flattened = []
    
    for crime in crimes:
        location = crime.get('location', {})
        street = location.get('street', {})
        outcome = crime.get('outcome_status', {})
        
        record = {
            'crime_id': crime.get('id'),
            'persistent_id': crime.get('persistent_id'),
            'category': crime.get('category'),
            'month': crime.get('month'),
            
            # Location
            'lat': location.get('latitude'),
            'lon': location.get('longitude'),
            'street_id': street.get('id'),
            'street_name': street.get('name'),
            'location_type': crime.get('location_type'),
            'location_subtype': crime.get('location_subtype'),
            
            # Outcome
            'outcome_category': outcome.get('category') if outcome else None,
            'outcome_date': outcome.get('date') if outcome else None,
            
            # Context
            'context': crime.get('context')
        }
        
        flattened.append(record)
    
    return flattened


def flatten_weather_data(weather: Dict) -> Dict:
    """
    Flatten OpenWeatherMap response into tabular format
    
    Args:
        weather: Weather data dict from OpenWeatherMap
    
    Returns:
        Flattened record dict
    """
    weather_info = weather.get('weather', [{}])[0]
    main = weather.get('main', {})
    wind = weather.get('wind', {})
    clouds = weather.get('clouds', {})
    sys = weather.get('sys', {})
    
    record = {
        # Location
        'location_name': weather.get('name'),
        'country': sys.get('country'),
        'lat': weather.get('coord', {}).get('lat'),
        'lon': weather.get('coord', {}).get('lon'),
        
        # Time
        'datetime': datetime.fromtimestamp(weather.get('dt', 0)).isoformat(),
        'timezone_offset': weather.get('timezone'),
        
        # Weather conditions
        'weather_id': weather_info.get('id'),
        'weather_main': weather_info.get('main'),
        'weather_description': weather_info.get('description'),
        'weather_icon': weather_info.get('icon'),
        
        # Temperature
        'temp_celsius': main.get('temp'),
        'feels_like_celsius': main.get('feels_like'),
        'temp_min_celsius': main.get('temp_min'),
        'temp_max_celsius': main.get('temp_max'),
        
        # Atmospheric conditions
        'pressure_hpa': main.get('pressure'),
        'humidity_percent': main.get('humidity'),
        'visibility_meters': weather.get('visibility'),
        
        # Wind
        'wind_speed_ms': wind.get('speed'),
        'wind_direction_deg': wind.get('deg'),
        'wind_gust_ms': wind.get('gust'),
        
        # Clouds
        'clouds_percent': clouds.get('all'),
        
        # Sun
        'sunrise': datetime.fromtimestamp(sys.get('sunrise', 0)).isoformat(),
        'sunset': datetime.fromtimestamp(sys.get('sunset', 0)).isoformat()
    }
    
    return record


def create_bronze_summary(output_dir: str, data_type: str, record_count: int, file_path: str):
    """
    Create a summary file for Bronze layer ingestion
    
    Args:
        output_dir: Bronze layer directory
        data_type: Type of data (infrastructure, crime, weather, etc.)
        record_count: Number of records ingested
        file_path: Path to data file
    """
    summary = {
        'data_type': data_type,
        'record_count': record_count,
        'ingestion_timestamp': datetime.now().isoformat(),
        'ingestion_date': datetime.now().strftime('%Y-%m-%d'),
        'file_path': file_path,
        'status': 'success' if record_count > 0 else 'no_data'
    }
    
    summary_path = os.path.join(output_dir, f"_summary_{data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"   📊 Summary saved to: {summary_path}")