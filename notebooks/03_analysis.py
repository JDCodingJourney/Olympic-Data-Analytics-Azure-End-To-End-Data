# Databricks Notebook - Data Analysis
# Tokyo 2020 Olympic Data Pipeline

print(" Starting data analysis...")

# Top 10 countries by total medals
print("\n Top 10 countries by total medals:")
medals.orderBy("Total", ascending=False).select("Team_Country", "Gold", "Silver", "Bronze", "Total").show(10)

# Gender participation analysis
print("\n Gender participation summary:")
gender_summary.show()

# Top 10 disciplines by participants
print("\n Top 10 disciplines by participants:")
entriesgender.orderBy("Total", ascending=False).select("Discipline", "Total", "Female", "Male").show(10)

# Countries with highest gold medal efficiency
print("\n Top 10 countries by gold medal efficiency (%):")
medals_with_efficiency.orderBy("Gold_Efficiency_Pct", ascending=False).select("Team_Country", "Gold", "Total", "Gold_Efficiency_Pct").show(10)

print("\n✅ Analysis completed!")
