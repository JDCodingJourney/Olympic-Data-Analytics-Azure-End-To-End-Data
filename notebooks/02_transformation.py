# Databricks Notebook - Data Transformation
# Tokyo 2021 Olympic Data Pipeline

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

print("🔄 Starting data transformations...")

# Fix column names in medals
medals = medals.withColumnRenamed("Team/NOC", "Team_Country")
print("✅ Renamed Team/NOC to Team_Country")

# Convert entriesgender columns to integer type
entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType())) \
    .withColumn("Male", col("Male").cast(IntegerType())) \
    .withColumn("Total", col("Total").cast(IntegerType()))
print("✅ Converted entriesgender columns to integer type")

# Calculate gold medal efficiency
medals_with_efficiency = medals.withColumn(
    "Gold_Efficiency_Pct", (col("Gold") / col("Total")) * 100
)
print("✅ Calculated gold medal efficiency")

# Calculate gender participation summary
from pyspark.sql.functions import sum

gender_summary = entriesgender.agg(
    sum("Female").alias("Total_Female"),
    sum("Male").alias("Total_Male"),
    sum("Total").alias("Total_Participants")
)

gender_summary.show()

print("\n✅ Transformations completed successfully!")
