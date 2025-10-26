# 🏙️ NUAR Mini - Infrastructure Data Hub

Medallion architecture pipeline for UK infrastructure data from multiple APIs.

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

3. Test APIs:
   ```bash
   python notebooks/00_test_all_apis.py
   ```

## Project Structure

```
nuar-mini-project/
├── notebooks/          # Main workflow notebooks
├── config/            # Configuration files
├── src/               # Reusable Python modules
│   ├── bronze/       # Raw data ingestion
│   ├── silver/       # Data cleaning
│   ├── gold/         # Analytics
│   └── utils/        # Helper functions
├── tests/            # Unit tests
└── data/             # Local data storage
```

## APIs Used

- Overpass API (Infrastructure)
- UK Police API (Crime Data)
- OpenWeatherMap (Weather)
- Postcodes.io (Geo lookups)

## Architecture

Bronze (Parquet) → Silver (Delta) → Gold (Optimized Delta)
