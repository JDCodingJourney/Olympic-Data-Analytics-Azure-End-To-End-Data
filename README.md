# Olympic Data Analytics Pipeline - End to End Data Engineering Project

## Project Overview
Complete ETL pipeline for Tokyo 2020 Olympic data using Microsoft Azure services.

## Architecture
- **Data Source**: Tokyo 2020 Olympic Data (CSV files)
- **Storage**: Azure Data Lake Storage Gen2
- **Processing**: Azure Databricks with PySpark
- **Analytics**: Azure Synapse Analytics

## Dataset
| File | Records | Description |
|------|---------|-------------|
| Athletes.csv | 11,085 | Athlete information |
| Coaches.csv | 394 | Coach information |
| EntriesGender.csv | 46 | Gender participation |
| Medals.csv | 93 | Medal counts |
| Teams.csv | 743 | Team composition |

## Technologies Used
- Azure Data Lake Storage Gen2
- Azure Databricks (PySpark)
- Azure Synapse Analytics
- Python

## Key Transformations
- Data type conversions
- Gender participation calculations
- Medal aggregations
- Business metrics

## Project Structure

├── data/raw/ # Raw CSV files
├── notebooks/ # Databricks PySpark code
├── sql/ # Synapse Analytics queries
└── docs/ # Documentation


## Author
José David Lammhider Baquero
