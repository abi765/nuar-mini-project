"""
Silver Layer Transformations
Reusable functions for transforming Bronze → Silver
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from pyproj import Transformer
from shapely.geometry import Point, LineString
from scipy.spatial import KDTree


# ============================================================================
# COORDINATE TRANSFORMATIONS
# ============================================================================

def create_coordinate_transformer():
    """
    Create transformer for WGS84 → British National Grid
    
    Returns:
        Transformer object (EPSG:4326 → EPSG:27700)
    """
    return Transformer.from_crs(
        "EPSG:4326",  # WGS84 (lat/lon)
        "EPSG:27700",  # British National Grid
        always_xy=True
    )


def transform_coordinates_to_bng(
    df: pd.DataFrame,
    lat_col: str = 'lat',
    lon_col: str = 'lon',
    drop_original: bool = False
) -> pd.DataFrame:
    """
    Transform WGS84 coordinates to British National Grid
    
    Args:
        df: DataFrame with lat/lon columns
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        drop_original: Whether to drop lat/lon after transformation
    
    Returns:
        DataFrame with easting_bng and northing_bng columns
    """
    print(f"🔄 Transforming {len(df)} coordinates to British National Grid...")
    
    transformer = create_coordinate_transformer()
    
    # Handle missing values
    valid_coords = df[[lat_col, lon_col]].notna().all(axis=1)
    
    if valid_coords.sum() == 0:
        print("   ⚠️  No valid coordinates to transform")
        df['easting_bng'] = np.nan
        df['northing_bng'] = np.nan
        return df
    
    # Transform valid coordinates
    valid_df = df[valid_coords]
    
    eastings, northings = transformer.transform(
        valid_df[lon_col].values,
        valid_df[lat_col].values
    )
    
    # Add to dataframe
    df['easting_bng'] = np.nan
    df['northing_bng'] = np.nan
    
    df.loc[valid_coords, 'easting_bng'] = eastings
    df.loc[valid_coords, 'northing_bng'] = northings
    
    # Round to integers (BNG is in meters)
    df['easting_bng'] = df['easting_bng'].round(0).astype('Int64')
    df['northing_bng'] = df['northing_bng'].round(0).astype('Int64')
    
    print(f"   ✅ Transformed {valid_coords.sum()} coordinates")
    
    if drop_original:
        df = df.drop(columns=[lat_col, lon_col])
        print(f"   🗑️  Dropped original lat/lon columns")
    
    return df


# ============================================================================
# GEOMETRY CALCULATIONS
# ============================================================================

def calculate_centroid_from_geometry(geometry_json: str) -> Tuple[float, float]:
    """
    Calculate centroid from OSM geometry JSON
    
    Args:
        geometry_json: JSON string with array of {lat, lon} points
    
    Returns:
        (lat, lon) tuple of centroid
    """
    if pd.isna(geometry_json):
        return np.nan, np.nan
    
    try:
        geom = json.loads(geometry_json)
        
        if not geom or len(geom) == 0:
            return np.nan, np.nan
        
        # Calculate average
        avg_lat = sum(p['lat'] for p in geom) / len(geom)
        avg_lon = sum(p['lon'] for p in geom) / len(geom)
        
        return avg_lat, avg_lon
    
    except (json.JSONDecodeError, KeyError, TypeError):
        return np.nan, np.nan


def calculate_line_length(geometry_json: str) -> Optional[float]:
    """
    Calculate length of a line from OSM geometry
    
    Args:
        geometry_json: JSON string with array of {lat, lon} points
    
    Returns:
        Length in meters, or None
    """
    if pd.isna(geometry_json):
        return None
    
    try:
        geom = json.loads(geometry_json)
        
        if not geom or len(geom) < 2:
            return None
        
        # Create LineString in BNG coordinates
        transformer = create_coordinate_transformer()
        
        coords_bng = [
            transformer.transform(p['lon'], p['lat'])
            for p in geom
        ]
        
        line = LineString(coords_bng)
        return line.length  # In meters
    
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def fix_infrastructure_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix missing coordinates in infrastructure data
    Calculates centroids for ways without explicit coordinates
    
    Args:
        df: Infrastructure DataFrame
    
    Returns:
        DataFrame with all coordinates filled
    """
    print(f"🔧 Fixing missing coordinates...")
    
    missing_coords = df['lat'].isna()
    n_missing = missing_coords.sum()
    
    if n_missing == 0:
        print(f"   ✅ No missing coordinates")
        return df
    
    print(f"   📍 Calculating centroids for {n_missing} records...")
    
    # Calculate centroids for ways
    df_missing = df[missing_coords].copy()
    
    centroids = df_missing['geometry_json'].apply(calculate_centroid_from_geometry)
    df.loc[missing_coords, 'lat'] = [c[0] for c in centroids]
    df.loc[missing_coords, 'lon'] = [c[1] for c in centroids]
    
    # Also use first_lat/first_lon as fallback
    still_missing = df['lat'].isna()
    df.loc[still_missing, 'lat'] = df.loc[still_missing, 'first_lat']
    df.loc[still_missing, 'lon'] = df.loc[still_missing, 'first_lon']
    
    fixed = n_missing - df['lat'].isna().sum()
    print(f"   ✅ Fixed {fixed} coordinates")
    
    return df


def calculate_infrastructure_length(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate length for line-based infrastructure (pipelines, cables)
    
    Args:
        df: Infrastructure DataFrame
    
    Returns:
        DataFrame with length_meters column
    """
    print(f"📏 Calculating infrastructure lengths...")
    
    # Only calculate for ways
    ways = df['type'] == 'way'
    
    if ways.sum() == 0:
        df['length_meters'] = np.nan
        print(f"   ⚠️  No ways to calculate length for")
        return df
    
    print(f"   🔍 Calculating lengths for {ways.sum()} ways...")
    
    df['length_meters'] = np.nan
    df.loc[ways, 'length_meters'] = df.loc[ways, 'geometry_json'].apply(
        calculate_line_length
    )
    
    calculated = df['length_meters'].notna().sum()
    print(f"   ✅ Calculated {calculated} lengths")
    
    return df


# ============================================================================
# DATA TYPE CONVERSIONS
# ============================================================================

def convert_string_coordinates_to_float(
    df: pd.DataFrame,
    lat_col: str = 'lat',
    lon_col: str = 'lon'
) -> pd.DataFrame:
    """
    Convert string coordinates to float (for crime data)
    
    Args:
        df: DataFrame with string coordinate columns
        lat_col: Name of latitude column
        lon_col: Name of longitude column
    
    Returns:
        DataFrame with numeric coordinates
    """
    print(f"🔢 Converting string coordinates to float...")
    
    original_valid = df[lat_col].notna().sum()
    
    # Convert to numeric
    df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
    df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')
    
    new_valid = df[lat_col].notna().sum()
    
    print(f"   ✅ Converted: {original_valid} → {new_valid} valid coordinates")
    
    return df


def parse_date_fields(
    df: pd.DataFrame,
    date_columns: List[str]
) -> pd.DataFrame:
    """
    Parse date strings to datetime
    
    Args:
        df: DataFrame
        date_columns: List of column names to parse
    
    Returns:
        DataFrame with parsed dates
    """
    print(f"📅 Parsing date fields...")
    
    for col in date_columns:
        if col not in df.columns:
            continue
        
        # Convert to string first if it's categorical or other type
        if df[col].dtype.name == 'category':
            df[col] = df[col].astype(str)
        
        # Handle month format (YYYY-MM)
        if col == 'month':
            df[col] = pd.to_datetime(df[col].astype(str) + '-01', errors='coerce')
        else:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        valid = df[col].notna().sum()
        print(f"   ✅ {col}: {valid} valid dates")
    
    return df


# ============================================================================
# SPATIAL ENRICHMENT
# ============================================================================

def spatial_join_nearest(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    left_coords: Tuple[str, str] = ('easting_bng', 'northing_bng'),
    right_coords: Tuple[str, str] = ('eastings', 'northings'),
    right_columns: List[str] = None,
    max_distance: float = 5000  # 5km max
) -> pd.DataFrame:
    """
    Join datasets by finding nearest neighbor in space
    
    Args:
        df_left: Left DataFrame (to enrich)
        df_right: Right DataFrame (reference data, e.g., postcodes)
        left_coords: (easting_col, northing_col) in left DataFrame
        right_coords: (easting_col, northing_col) in right DataFrame
        right_columns: Columns to add from right DataFrame
        max_distance: Maximum distance for match (meters)
    
    Returns:
        Left DataFrame with enriched columns
    """
    print(f"🗺️  Spatial join: {len(df_left)} records → nearest of {len(df_right)} points")
    
    # Prepare coordinates
    left_coords_array = df_left[list(left_coords)].dropna().values
    right_coords_array = df_right[list(right_coords)].dropna().values
    
    if len(left_coords_array) == 0 or len(right_coords_array) == 0:
        print("   ⚠️  No valid coordinates for spatial join")
        return df_left
    
    # Build KDTree for fast nearest neighbor search
    tree = KDTree(right_coords_array)
    
    # Find nearest for each point in left
    valid_left = df_left[list(left_coords)].notna().all(axis=1)
    distances, indices = tree.query(df_left.loc[valid_left, list(left_coords)].values)
    
    # Filter by max distance
    within_distance = distances <= max_distance
    
    print(f"   ✅ Matched {within_distance.sum()} within {max_distance}m")
    
    # Add enrichment columns
    if right_columns is None:
        right_columns = ['postcode', 'admin_district', 'admin_ward']
    
    for col in right_columns:
        if col not in df_right.columns:
            continue
        
        # Initialize column with NaN
        df_left[col] = np.nan
        
        # Create array to hold all values (same length as valid_left)
        values_to_assign = np.full(valid_left.sum(), np.nan, dtype=object)
        
        # Only assign matched values where within distance
        matched_indices = indices[within_distance]
        matched_values = df_right.iloc[matched_indices][col].values
        
        # Assign matched values to correct positions
        values_to_assign[within_distance] = matched_values
        
        # Assign to dataframe
        df_left.loc[valid_left, col] = values_to_assign
    
    return df_left


# ============================================================================
# DATA QUALITY
# ============================================================================

def calculate_metadata_quality_score(
    df: pd.DataFrame,
    metadata_columns: List[str]
) -> pd.Series:
    """
    Calculate metadata quality score (0-1) based on field completeness
    
    Args:
        df: DataFrame
        metadata_columns: List of metadata columns to check
    
    Returns:
        Series with quality scores (0-1)
    """
    available_cols = [c for c in metadata_columns if c in df.columns]
    
    if not available_cols:
        return pd.Series(0.0, index=df.index)
    
    scores = df[available_cols].notna().sum(axis=1) / len(available_cols)
    
    return scores


def add_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add data quality flags to DataFrame
    
    Args:
        df: DataFrame
    
    Returns:
        DataFrame with quality flag columns
    """
    print(f"🏷️  Adding quality flags...")
    
    # Coordinate quality
    if 'lat' in df.columns and 'lon' in df.columns:
        df['has_coordinates'] = df[['lat', 'lon']].notna().all(axis=1)
    
    # BNG coordinate quality
    if 'easting_bng' in df.columns:
        df['has_bng_coords'] = df[['easting_bng', 'northing_bng']].notna().all(axis=1)
    
    # Enrichment quality
    if 'postcode' in df.columns:
        df['has_postcode'] = df['postcode'].notna()
    
    if 'admin_district' in df.columns:
        df['has_admin_data'] = df['admin_district'].notna()
    
    print(f"   ✅ Added quality flags")
    
    return df


def validate_stockport_bbox(
    df: pd.DataFrame,
    bbox: Dict = None
) -> pd.DataFrame:
    """
    Validate records are within Stockport bounding box
    
    Args:
        df: DataFrame with lat/lon
        bbox: Bounding box dict (default: Stockport)
    
    Returns:
        DataFrame with is_valid_location flag
    """
    if bbox is None:
        bbox = {
            'lat_min': 53.35,
            'lat_max': 53.45,
            'lon_min': -2.20,
            'lon_max': -2.05
        }
    
    print(f"✅ Validating coordinates within bbox...")
    
    valid = (
        (df['lat'] >= bbox['lat_min']) &
        (df['lat'] <= bbox['lat_max']) &
        (df['lon'] >= bbox['lon_min']) &
        (df['lon'] <= bbox['lon_max'])
    )
    
    df['is_valid_location'] = valid
    
    valid_count = valid.sum()
    print(f"   ✅ {valid_count}/{len(df)} within Stockport bbox")
    
    return df


# ============================================================================
# SCHEMA ENFORCEMENT
# ============================================================================

def enforce_schema(
    df: pd.DataFrame,
    schema: Dict[str, str]
) -> pd.DataFrame:
    """
    Enforce data types according to schema
    
    Args:
        df: DataFrame
        schema: Dict of {column_name: dtype}
    
    Returns:
        DataFrame with enforced types
    """
    print(f"📋 Enforcing schema...")
    
    for col, dtype in schema.items():
        if col not in df.columns:
            continue
        
        try:
            if dtype == 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif dtype == 'category':
                df[col] = df[col].astype('category')
            elif dtype == 'Int64':
                df[col] = df[col].astype('Int64')
            else:
                df[col] = df[col].astype(dtype)
        
        except Exception as e:
            print(f"   ⚠️  Could not convert {col} to {dtype}: {e}")
    
    print(f"   ✅ Schema enforced")
    
    return df


# ============================================================================
# AUDIT COLUMNS
# ============================================================================

def add_audit_columns(
    df: pd.DataFrame,
    source_system: str,
    layer: str = 'silver'
) -> pd.DataFrame:
    """
    Add audit/tracking columns
    
    Args:
        df: DataFrame
        source_system: Name of source system
        layer: Data layer name
    
    Returns:
        DataFrame with audit columns
    """
    df['transformation_timestamp'] = datetime.now().isoformat()
    df['transformation_date'] = datetime.now().strftime('%Y-%m-%d')
    df['data_layer'] = layer
    df['source_system'] = source_system
    
    return df