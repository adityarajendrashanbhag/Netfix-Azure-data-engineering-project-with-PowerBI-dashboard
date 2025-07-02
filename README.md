
# Netflix Data Engineering: End-to-End Pipeline & Power BI Dashboard

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture Diagram](#architecture-diagram)
- [Technology Stack](#technology-stack)
- [Pipeline Stages](#pipeline-stages)
  - [Bronze: Data Ingestion](#bronze-data-ingestion)
  - [Silver: Data Cleansing & Transformation](#silver-data-cleansing--transformation)
  - [Gold: Aggregation & Denormalization](#gold-aggregation--denormalization)
- [Power BI Dashboard Highlights](#power-bi-dashboard-highlights)
- [License](#license)

---

## Project Overview
This repository demonstrates an end-to-end data engineering solution on Azure, where we build ETL pipelines using the Medallion Architecture to process raw Netflix catalog data and transform it into actionable insights delivered through a Power BI dashboard.

Key outcomes include:
- Automated ingestion of Netflix data (hosted on GitHub, sourced from Kaggle) into Azure Data Lake Gen2.
- Structured ETL pipelines using Azure Databricks and Delta Lake to ensure data reliability and scalability.
- An interactive Power BI dashboard of the Netflix content catalog, featuring genre distributions, audience rating breakdowns, time-based content trends, and geographic spread across top countries.

---

## Architecture Diagram

![Netflix Data Engineering Architecture](https://raw.githubusercontent.com/adityarajendrashanbhag/Netfix-Azure-data-engineering-project-with-PowerBI-dashboard/main/netflix-data-engineering-architecture.jpg)


---

## Technical stack
- **Azure Data Lake Gen2**: storage for each Medallion layer.
- **Azure Databricks & Delta Lake**: Spark-based ETL data processing.
- **Unity Catalog**: for managing Delta table governance and security.
- **Power BI**: interactive reporting and visualization of Netflix data.
- **Python & PySpark**: notebook-driven data processing for transformation of raw data.

---

## Pipeline Stages

### Bronze: Data Ingestion from Github API to ADLS Gen2
- **Source**: GitHub Netflix Titles Dataset API  
- **Destination**: `bronze-ma/netflix-{file_name}` on ADLS Gen2 using parametrized   
- **Process**: We use ADF to call the Github API and once the request passes and it is true, we extract the data into bronze-ma folder
![ADF](https://raw.githubusercontent.com/adityarajendrashanbhag/Netfix-Azure-data-engineering-project-with-PowerBI-dashboard/main/azure-data-factory/ADF_pipeline.jpg) 

### Silver: Data Cleansing & Transformation  
- **Input**: Bronze JSON  
- **Tech**: PySpark in Databricks  
- **Steps**:
  1. Infer and apply schema.  
  2. Cleanse null or malformed records.  
  3. Standardize types and formats.  
  4. Persist as Delta tables in `silver/netflix/`.  

### Gold: Aggregation & Denormalization  
- **Input**: Silver Delta tables (titles, directors, cast, countries, categories)  
- **Process**:
  1. Join dimension tables into a fact table.  
  2. Calculate business metrics (e.g., average runtime, season counts).  
  3. Save final Delta table at `gold/netflix_analytics/`.  

---

## Power BI Dashboard Highlights
With 6,234 titles, movies make up nearly 68% of the catalog, while TV shows account for the remaining 32%. Over the past two decades, the content library has grown steadily, revealing Netflix’s relentless push to expand its offerings year over year.
Diving deeper into genres, documentaries top the list, followed by stand-up comedy, kids’ TV, and international dramas. On the map, the United States dominates the content count (over 31%), trailed by India, the UK, Japan, Canada, and Mexico—highlighting Netflix’s global reach and strategic market focus.
Interestingly, the catalog is tailored toward mature viewers, with TV-MA and TV-14 ratings leading by a large margin, shaping the platform as a destination for adult audiences. Meanwhile, the average movie runs 99 minutes, and TV shows typically hover around 2 seasons, suggesting a rich mix of quick-hit movies and binge-ready series.
This dashboard captures a clear picture of how Netflix’s content strategy balances volume, variety, and audience focus across different genres and geographies. 


*Dashboard file: `netflix-analytics-dashboard-power-bi.pbix`*


![Netflix Analytics Dashboard](https://raw.githubusercontent.com/adityarajendrashanbhag/Netfix-Azure-data-engineering-project-with-PowerBI-dashboard/main/analytics-dashboard/netflix-analytics-dashboard-power-bi.jpg)


---

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
