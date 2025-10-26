#!/bin/bash
# Script to restructure NUAR Mini Project

echo "🏗️  Restructuring NUAR Mini Project..."

# Navigate to project root (adjust if needed)
cd ~/Downloads/PersonalProjects/NUARmini_OverpassAPI

# Create folder structure
echo "📁 Creating folder structure..."
mkdir -p notebooks
mkdir -p config
mkdir -p src/{bronze,silver,gold,utils}
mkdir -p tests
mkdir -p data/{sample,schemas}
mkdir -p docs
mkdir -p scripts

# Move existing files
echo "📦 Moving existing files..."
mv overpassAPIcheck.py notebooks/00_test_overpass.py
mv test_all_apis_local.py notebooks/00_test_all_apis.py

# Keep requirements.txt and .env in root
echo "✅ Keeping requirements.txt and .env in root"

# Create __init__.py files for Python packages
echo "🐍 Creating Python package files..."
touch config/__init__.py
touch src/__init__.py
touch src/utils/__init__.py
touch src/bronze/__init__.py
touch src/silver/__init__.py
touch src/gold/__init__.py
touch tests/__init__.py

# Create .gitignore
echo "🚫 Creating .gitignore..."
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
env/
*.egg-info/

# Environment variables
.env
.env.local

# Databricks
.databricks/
*.dbc

# Data files
data/raw/
data/processed/
*.parquet
*.delta

# Secrets
config/api_keys.json
*.key

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db

# Logs
logs/
*.log

# Testing
.pytest_cache/
.coverage
EOF

# Create README.md
echo "📝 Creating README.md..."
cat > README.md << 'EOF'
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
EOF

# Create .env.example template
echo "🔑 Creating .env.example..."
cat > .env.example << 'EOF'
# OpenWeatherMap API Key
OPENWEATHER_API_KEY=your_api_key_here

# Optional: Databricks connection (for later)
# DATABRICKS_HOST=your_workspace_url
# DATABRICKS_TOKEN=your_token
EOF

echo ""
echo "✅ Project restructured successfully!"
echo ""
echo "📂 New structure:"
tree -I "venv" -L 2

echo ""
echo "🎯 Next steps:"
echo "1. Review the new structure"
echo "2. Update notebooks/00_test_all_apis.py to use .env"
echo "3. Initialize Git: git init"
echo "4. Make first commit: git add . && git commit -m 'Initial project structure'"