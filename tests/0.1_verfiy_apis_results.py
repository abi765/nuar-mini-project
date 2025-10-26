import pandas as pd
from tabulate import tabulate

def print_table(title, df, group_col, count_name="count"):
    print("=" * 80)
    print(f"✅ {title.upper()} SUMMARY")
    print("=" * 80)

    # Group and count
    summary = (
        df[group_col]
        .value_counts()
        .reset_index()
        .rename(columns={"index": group_col, group_col: count_name})
    )
    
    print(f"Total {title.lower()}: {len(df)} records\n")
    print(tabulate(summary, headers="keys", tablefmt="github", showindex=False))
    print("\n")


print("="*80)
print("🏗️  NUAR MINI PROJECT — BRONZE DATA SUMMARY")
print("="*80)
print("\n")

# 1️⃣ Infrastructure
infra = pd.read_parquet("data/bronze/stockport/infrastructure/parquet/infrastructure_stockport")
print_table("Infrastructure", infra, "infrastructure_type")

# 2️⃣ Crime
crime = pd.read_parquet("data/bronze/stockport/crime/parquet/crimes_stockport")
print_table("Crime", crime, "category")

# 3️⃣ Weather
weather = pd.read_parquet("data/bronze/stockport/weather/parquet/weather_stockport")
print_table("Weather", weather, "ingestion_date")

# 4️⃣ Postcodes
postcodes = pd.read_parquet("data/bronze/stockport/postcodes/parquet/postcodes_stockport")
print_table("Postcodes", postcodes, "ingestion_date")