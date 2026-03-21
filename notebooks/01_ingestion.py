# Databricks Notebook - Data Ingestion
# Tokyo 2021 Olympic Data Pipeline

from pyspark.sql.functions import col

# Configuration
storage_account = "tokyolympicdate"
container = "tokyo-olympic-data"
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
raw_data_path = f"{base_path}/raw-data"

print(f"Loading data from: {raw_data_path}")

# Load athletes data
athletes = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(f"{raw_data_path}/Atheltes.csv")

# Load coaches data
coaches = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(f"{raw_data_path}/Coaches.csv")

# Load entries gender data
entriesgender = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(f"{raw_data_path}/Entriesgender.csv")

# Load medals data
medals = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(f"{raw_data_path}/Medals.csv")

# Load teams data
teams = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(f"{raw_data_path}/Teams.csv")

# Verify data loading
print("\n✅ Data loaded successfully!")
print(f"athletes: {athletes.count()} records")
print(f"coaches: {coaches.count()} records")
print(f"entriesgender: {entriesgender.count()} records")
print(f"medals: {medals.count()} records")
print(f"teams: {teams.count()} records")
