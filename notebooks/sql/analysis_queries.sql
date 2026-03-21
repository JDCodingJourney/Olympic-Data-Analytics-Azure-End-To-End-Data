-- SQL Queries for Azure Synapse Analytics
-- Tokyo 2020 Olympic Data Analysis

-- 1. Top 10 countries by total medals
SELECT TOP 10
    Team_Country,
    Gold,
    Silver,
    Bronze,
    Total
FROM medals
ORDER BY Total DESC;

-- 2. Gender participation by discipline
SELECT 
    Discipline,
    Female,
    Male,
    Total,
    CAST(Female AS FLOAT) / Total * 100 AS Female_Percentage,
    CAST(Male AS FLOAT) / Total * 100 AS Male_Percentage
FROM entriesgender
ORDER BY Total DESC;

-- 3. Countries with highest gold efficiency
SELECT TOP 10
    Team_Country,
    Gold,
    Total,
    CAST(Gold AS FLOAT) / Total * 100 AS Gold_Efficiency_Pct
FROM medals
WHERE Total > 0
ORDER BY Gold_Efficiency_Pct DESC;

-- 4. Athletes distribution by discipline
SELECT 
    Discipline,
    COUNT(*) AS Athlete_Count
FROM athletes
GROUP BY Discipline
ORDER BY Athlete_Count DESC;
