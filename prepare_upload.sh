#!/bin/bash
# Prepare Bronze data for manual upload to Databricks

echo "================================================================================"
echo "📦 PREPARING BRONZE DATA FOR DATABRICKS UPLOAD"
echo "================================================================================"
echo ""

# Check if data exists
if [ ! -d "data/bronze/stockport" ]; then
    echo "❌ Error: data/bronze/stockport/ directory not found"
    echo ""
    echo "Please run data collection first:"
    echo "  python run_bronze_collection.py"
    echo ""
    exit 1
fi

# Count files
echo "📊 Checking local data..."
echo ""

for domain in infrastructure crime weather postcodes; do
    if [ -d "data/bronze/stockport/$domain" ]; then
        file_count=$(find "data/bronze/stockport/$domain" -type f | wc -l | tr -d ' ')
        echo "   ✅ $domain: $file_count files"
    else
        echo "   ⚠️  $domain: not found"
    fi
done

echo ""
echo "📦 Creating zip archive..."
echo ""

# Create zip (exclude any existing zips)
cd data/bronze
zip -r ../../bronze_data.zip stockport/ -x "*.zip"
cd ../..

if [ -f "bronze_data.zip" ]; then
    zip_size=$(du -h bronze_data.zip | cut -f1)
    echo "   ✅ Created: bronze_data.zip ($zip_size)"
    echo ""
    echo "================================================================================"
    echo "✅ READY FOR UPLOAD"
    echo "================================================================================"
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. Upload to Databricks:"
    echo "   - Go to your Databricks workspace"
    echo "   - Click 'Data' in the left sidebar"
    echo "   - Click 'Upload File' or 'Add' → 'Upload File'"
    echo "   - Select: bronze_data.zip"
    echo "   - Note the upload path (e.g., dbfs:/FileStore/bronze_data.zip)"
    echo ""
    echo "2. Run the upload notebook:"
    echo "   - Navigate to: databricks_notebooks/utils/upload_local_data.py"
    echo "   - Update UPLOADED_ZIP_PATH with your actual upload path"
    echo "   - Run all cells"
    echo ""
    echo "3. The notebook will extract and organize your data to:"
    echo "   dbfs:/FileStore/nuar/bronze/stockport/"
    echo ""
    echo "================================================================================"
else
    echo "   ❌ Failed to create zip file"
    exit 1
fi
