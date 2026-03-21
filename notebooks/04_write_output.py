# Databricks Notebook - Write Output
# Tokyo 2020 Olympic Data Pipeline

transformed_data_path = f"{base_path}/transformed-data"

print(f"💾 Saving transformed data to: {transformed_data_path}")

# Save all transformed data
athletes.repartition(1).write.mode("overwrite").option("header", 'true').csv(f"{transformed_data_path}/athletes")
print("✅ Saved athletes data")

coaches.repartition(1).write.mode("overwrite").option("header", "true").csv(f"{transformed_data_path}/coaches")
print("✅ Saved coaches data")

entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv(f"{transformed_data_path}/entriesgender")
print("✅ Saved entriesgender data")

medals.repartition(1).write.mode("overwrite").option("header", "true").csv(f"{transformed_data_path}/medals")
print("✅ Saved medals data")

teams.repartition(1).write.mode("overwrite").option("header", "true").csv(f"{transformed_data_path}/teams")
print("✅ Saved teams data")

# Save summary tables
gender_summary.repartition(1).write.mode("overwrite").option("header", "true").csv(f"{transformed_data_path}/gender_summary")
print("✅ Saved gender summary")

medals_with_efficiency.repartition(1).write.mode("overwrite").option("header", "true").csv(f"{transformed_data_path}/medals_with_efficiency")
print("✅ Saved medals with efficiency")

print(f"\n✅ All data saved to: {transformed_data_path}")
