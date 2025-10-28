"""
Databricks-specific configuration for NUAR Mini Project
Overrides local settings when running in Databricks environment
"""

import os
import json
from pathlib import Path

# Detect if running in Databricks
try:
    import dbutils
    IS_DATABRICKS = True
except:
    IS_DATABRICKS = False

# ============================================================================
# API KEYS - Retrieved from Databricks Secrets
# ============================================================================

if IS_DATABRICKS:
    try:
        OPENWEATHER_API_KEY = dbutils.secrets.get(scope="nuar_secrets", key="openweather_api_key")
    except:
        # Fallback to environment variable
        OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
else:
    # Local development - use .env file
    from dotenv import load_dotenv
    load_dotenv()
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

# ============================================================================
# DATABRICKS CATALOG AND SCHEMA SETTINGS
# ============================================================================

CATALOG_NAME = "nuar_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Full schema paths
BRONZE_SCHEMA_FULL = f"{CATALOG_NAME}.{BRONZE_SCHEMA}"
SILVER_SCHEMA_FULL = f"{CATALOG_NAME}.{SILVER_SCHEMA}"
GOLD_SCHEMA_FULL = f"{CATALOG_NAME}.{GOLD_SCHEMA}"

# ============================================================================
# STORAGE PATHS
# ============================================================================

if IS_DATABRICKS:
    # DBFS paths for Databricks
    BASE_PATH = "/dbfs/FileStore/nuar"
    BRONZE_BASE_PATH = f"{BASE_PATH}/{BRONZE_SCHEMA}"
    SILVER_BASE_PATH = f"{BASE_PATH}/{SILVER_SCHEMA}"
    GOLD_BASE_PATH = f"{BASE_PATH}/{GOLD_SCHEMA}"

    # Delta table paths
    BRONZE_DELTA_PATH = f"dbfs:/FileStore/nuar/{BRONZE_SCHEMA}"
    SILVER_DELTA_PATH = f"dbfs:/FileStore/nuar/{SILVER_SCHEMA}"
    GOLD_DELTA_PATH = f"dbfs:/FileStore/nuar/{GOLD_SCHEMA}"
else:
    # Local filesystem paths
    PROJECT_ROOT = Path(__file__).parent.parent
    BASE_PATH = PROJECT_ROOT / "data"
    BRONZE_BASE_PATH = BASE_PATH / "bronze"
    SILVER_BASE_PATH = BASE_PATH / "silver"
    GOLD_BASE_PATH = BASE_PATH / "gold"

    BRONZE_DELTA_PATH = str(BRONZE_BASE_PATH)
    SILVER_DELTA_PATH = str(SILVER_BASE_PATH)
    GOLD_DELTA_PATH = str(GOLD_BASE_PATH)

# ============================================================================
# API ENDPOINTS
# ============================================================================

OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
POLICE_API_BASE = "https://data.police.uk/api"
WEATHER_API_BASE = "https://api.openweathermap.org/data/2.5"
POSTCODES_API_BASE = "https://api.postcodes.io"

# Backup Overpass endpoints
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter"
]

# ============================================================================
# STOCKPORT CONFIGURATION
# ============================================================================

# Load Stockport config
if IS_DATABRICKS:
    # In Databricks, use hardcoded config (avoid path issues)
    # The full config is embedded to ensure availability
    STOCKPORT_CONFIG = {
        'stockport': {
            'name': 'Stockport',
            'council': 'Stockport Metropolitan Borough Council',
            'area_km2': 126.0,
            'bounding_box': {
                'south': 53.35,
                'west': -2.20,
                'north': 53.45,
                'east': -2.05
            },
            'center': {
                'lat': 53.40,
                'lon': -2.15
            },
            'infrastructure_focus': [
                'manhole',
                'pipeline',
                'power_cable',
                'cable',
                'duct',
                'utility_pole'
            ]
        }
    }
else:
    CONFIG_DIR = Path(__file__).parent
    with open(CONFIG_DIR / "stockport.json") as f:
        STOCKPORT_CONFIG = json.load(f)

STOCKPORT_BBOX = STOCKPORT_CONFIG['stockport']['bounding_box']
STOCKPORT_CENTER = STOCKPORT_CONFIG['stockport']['center']
STOCKPORT_INFRASTRUCTURE_TYPES = STOCKPORT_CONFIG['stockport'].get(
    'infrastructure_focus',
    ['manhole', 'pipeline', 'power_cable', 'cable', 'duct', 'utility_pole']
)

# ============================================================================
# DELTA LAKE TABLE NAMES
# ============================================================================

# Bronze tables
BRONZE_TABLES = {
    'infrastructure': f"{BRONZE_SCHEMA_FULL}.infrastructure",
    'crime': f"{BRONZE_SCHEMA_FULL}.crime",
    'weather': f"{BRONZE_SCHEMA_FULL}.weather",
    'postcodes': f"{BRONZE_SCHEMA_FULL}.postcodes"
}

# Silver tables
SILVER_TABLES = {
    'infrastructure': f"{SILVER_SCHEMA_FULL}.infrastructure",
    'crime': f"{SILVER_SCHEMA_FULL}.crime",
    'weather': f"{SILVER_SCHEMA_FULL}.weather",
    'postcodes': f"{SILVER_SCHEMA_FULL}.postcodes"
}

# Gold tables (planned)
GOLD_TABLES = {
    'infrastructure_density': f"{GOLD_SCHEMA_FULL}.infrastructure_density",
    'crime_hotspots': f"{GOLD_SCHEMA_FULL}.crime_hotspots",
    'weather_trends': f"{GOLD_SCHEMA_FULL}.weather_trends",
    'spatial_correlations': f"{GOLD_SCHEMA_FULL}.spatial_correlations"
}

# ============================================================================
# DATABRICKS CLUSTER SETTINGS
# ============================================================================

# Recommended cluster configuration
CLUSTER_CONFIG = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.databricks.delta.optimizeWrite.enabled': 'true',
    'spark.databricks.delta.autoCompact.enabled': 'true'
}

# ============================================================================
# PROCESSING SETTINGS
# ============================================================================

# API timeout settings (seconds)
OVERPASS_TIMEOUT = 600  # 10 minutes for large queries
POLICE_API_TIMEOUT = 30
WEATHER_API_TIMEOUT = 30
POSTCODES_API_TIMEOUT = 30

# Rate limiting (seconds between requests)
POLICE_API_RATE_LIMIT = 1.0
POSTCODES_API_RATE_LIMIT = 0.5

# Grid settings for spatial queries
CRIME_GRID_SIZE = 3  # 3x3 grid for crime queries

# Data quality thresholds
MIN_COORDINATE_COMPLETENESS = 0.90  # 90% of records must have coordinates
MIN_BNG_TRANSFORMATION_SUCCESS = 0.95  # 95% must transform successfully
MAX_BBOX_OUTLIERS = 0.05  # Max 5% of records outside bounding box

# ============================================================================
# COORDINATE SYSTEMS
# ============================================================================

# Source coordinate system (from APIs)
SOURCE_CRS = "EPSG:4326"  # WGS84 (latitude/longitude)

# Target coordinate system (for analysis)
TARGET_CRS = "EPSG:27700"  # British National Grid (meters)

# Stockport bounding box in BNG (approximate)
STOCKPORT_BNG_BBOX = {
    'min_easting': 385000,
    'max_easting': 395000,
    'min_northing': 385000,
    'max_northing': 395000
}

# ============================================================================
# LOGGING AND MONITORING
# ============================================================================

# Log level
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR

# Enable detailed API logging
ENABLE_API_LOGGING = True

# Enable performance metrics
ENABLE_PERFORMANCE_METRICS = True

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_table_path(layer: str, table_name: str) -> str:
    """
    Get full Delta table path

    Args:
        layer: 'bronze', 'silver', or 'gold'
        table_name: Name of the table

    Returns:
        Full table path
    """
    if layer == 'bronze':
        return f"{BRONZE_SCHEMA_FULL}.{table_name}"
    elif layer == 'silver':
        return f"{SILVER_SCHEMA_FULL}.{table_name}"
    elif layer == 'gold':
        return f"{GOLD_SCHEMA_FULL}.{table_name}"
    else:
        raise ValueError(f"Invalid layer: {layer}")


def get_storage_path(layer: str, table_name: str) -> str:
    """
    Get physical storage path for table

    Args:
        layer: 'bronze', 'silver', or 'gold'
        table_name: Name of the table

    Returns:
        DBFS or local path
    """
    if layer == 'bronze':
        return f"{BRONZE_DELTA_PATH}/{table_name}"
    elif layer == 'silver':
        return f"{SILVER_DELTA_PATH}/{table_name}"
    elif layer == 'gold':
        return f"{GOLD_DELTA_PATH}/{table_name}"
    else:
        raise ValueError(f"Invalid layer: {layer}")


def print_config_summary():
    """Print configuration summary for debugging"""
    print("=" * 80)
    print("NUAR Mini - Databricks Configuration")
    print("=" * 80)
    print(f"Environment: {'Databricks' if IS_DATABRICKS else 'Local'}")
    print(f"Catalog: {CATALOG_NAME}")
    print(f"Bronze Schema: {BRONZE_SCHEMA_FULL}")
    print(f"Silver Schema: {SILVER_SCHEMA_FULL}")
    print(f"Gold Schema: {GOLD_SCHEMA_FULL}")
    print(f"Base Storage Path: {BASE_PATH}")
    print(f"API Key Configured: {'Yes' if OPENWEATHER_API_KEY else 'No'}")
    print("=" * 80)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    'IS_DATABRICKS',
    'OPENWEATHER_API_KEY',
    'CATALOG_NAME',
    'BRONZE_SCHEMA',
    'SILVER_SCHEMA',
    'GOLD_SCHEMA',
    'BRONZE_SCHEMA_FULL',
    'SILVER_SCHEMA_FULL',
    'GOLD_SCHEMA_FULL',
    'BRONZE_BASE_PATH',
    'SILVER_BASE_PATH',
    'GOLD_BASE_PATH',
    'BRONZE_DELTA_PATH',
    'SILVER_DELTA_PATH',
    'GOLD_DELTA_PATH',
    'OVERPASS_ENDPOINT',
    'OVERPASS_ENDPOINTS',
    'POLICE_API_BASE',
    'WEATHER_API_BASE',
    'POSTCODES_API_BASE',
    'STOCKPORT_CONFIG',
    'STOCKPORT_BBOX',
    'STOCKPORT_CENTER',
    'STOCKPORT_INFRASTRUCTURE_TYPES',
    'BRONZE_TABLES',
    'SILVER_TABLES',
    'GOLD_TABLES',
    'OVERPASS_TIMEOUT',
    'POLICE_API_TIMEOUT',
    'WEATHER_API_TIMEOUT',
    'POSTCODES_API_TIMEOUT',
    'SOURCE_CRS',
    'TARGET_CRS',
    'get_table_path',
    'get_storage_path',
    'print_config_summary'
]
