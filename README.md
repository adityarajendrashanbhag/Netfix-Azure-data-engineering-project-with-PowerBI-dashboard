
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
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Future Enhancements](#future-enhancements)
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

![Netflix-DE-Architecture](https://raw.githubusercontent.com/adityarajendrashanbhag/repo/main/images/netflix-data-engineering-architecture.jpg)


---

## Technology Stack
- **Azure Data Lake Gen2**: scalable storage for each medallion layer.
- **Azure Databricks & Delta Lake**: Spark-based ETL, ACID transactions.
- **Unity Catalog**: Delta table governance and security.
- **Power BI**: interactive reporting and visualization.
- **Python & PySpark**: notebook-driven data processing.

---

## Pipeline Stages

### Bronze: Data Ingestion  
- **Source**: GitHub Netflix Titles Dataset API  
- **Destination**: `bronze/netflix/` on ADLS Gen2  
- **Process**: Databricks notebook calls the API and stores raw JSON.  

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
- **Library Pulse**: overall content count and recent additions.  
- **Composition & Reach**: genre breakdowns and top country map.  
- **Performance Spotlight**: runtime averages and season distributions.  

*Dashboard file: `Netflix_Analytics.pbix`*

---

## Getting Started

1. **Clone repository**:
   ```bash
   git clone https://github.com/your-org/netflix-data-engineering.git
   cd netflix-data-engineering
   ```
2. **Configure Azure credentials**:
   ```bash
   export AZURE_STORAGE_ACCOUNT=<your-account>
   export AZURE_CLIENT_ID=<your-client-id>
   export AZURE_CLIENT_SECRET=<your-secret>
   export AZURE_TENANT_ID=<your-tenant-id>
   ```
3. **Import Databricks notebooks** from `notebooks/` and attach to a cluster.
4. **Open Power BI**: load `reports/Netflix_Analytics.pbix` and connect to your Gold Delta endpoint.

---

## Project Structure
```
netflix-data-engineering/
├── notebooks/        
│   ├ 01_bronze.ipynb
│   ├ 02_silver.ipynb
│   └ 03_gold.ipynb
├── analytics-dashboard/          
│   └ netflix-analytics-dashboard-power-bi.pbix
|   └ netflix-analytics-dashboard-power-bi.pdf
├── dataset/             
│   └ dimension_table
│               └ netflix_cast.csv
│               └ netflix_category.csv
│               └ netflix_countries.csv
│               └ netflix_directors.csv
│   └ fact_table
│               └ netflix_titles.csv
├── README.md
└── LICENSE
```

---

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
