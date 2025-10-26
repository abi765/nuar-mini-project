"""
Central configuration for Stockport NUAR project
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"

# Load Stockport configuration
with open(CONFIG_DIR / "stockport.json") as f:
    STOCKPORT_CONFIG = json.load(f)

# API Keys
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

# Databricks settings (for later)
CATALOG_NAME = "nuar_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# API Endpoints
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"
POLICE_API_BASE = "https://data.police.uk/api"
WEATHER_API_BASE = "https://api.openweathermap.org/data/2.5"
POSTCODES_API_BASE = "https://api.postcodes.io"

# Stockport specific
STOCKPORT_BBOX = STOCKPORT_CONFIG['stockport']['bounding_box']
STOCKPORT_CENTER = STOCKPORT_CONFIG['stockport']['center']