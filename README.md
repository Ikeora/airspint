# ✈️ AirSprint Flight Data Analytics 

## 📌 Project Overview
This project implements an end-to-end cloud-based data engineering and business intelligence pipeline for analyzing flight operations and aircraft ownership data. The solution focuses on automated data ingestion, transformation, storage, and near real-time visualization using modern analytics and cloud technologies.

The architecture includes real-world production data platforms, emphasizing scalability, automation, and analytics-ready data modeling.

---

## 🏗️ Architecture Overview
The pipeline follows a layered, cloud-native design:

External API → Azure Blob Storage → Azure Functions → Azure Data Factory → Azure SQL Database → Power BI

This approach ensures reliable ingestion, structured transformations, and fast access to business insights.

---

## 🔄 Data Pipeline

### 1️⃣ Data Ingestion
- A Python-based API ingestion script retrieves flight data for selected flight IDs.
- Raw API responses are stored in Azure Blob Storage as a centralized landing zone.
- Supplementary datasets such as aircraft details and ownership information are also stored in the same storage account.

**Script:**

---

### 2️⃣ Data Transformation & Enrichment

#### Python ETL (Azure Functions)
- A reusable Python ETL module cleans and transforms raw datasets.
- The script is deployed as an Azure Function App to enable serverless execution and automated scaling.

**Key transformations include:**
- Handling missing and inconsistent values  
- Removing duplicates  
- Standardizing date and numeric formats  
- Feature engineering (e.g., flight duration, ownership percentage calculations)

**Script:**

---

#### Orchestration with Azure Data Factory
- An Azure Data Factory pipeline orchestrates the transformation process.
- The pipeline reads raw data from Blob Storage, executes the Python transformation logic, and writes processed data to a clean storage layer.

---

### 3️⃣ Data Modeling & Storage
- Cleaned datasets are loaded into an Azure SQL Database.
- Additional transformations are applied using SQL:
  - Joining flight and aircraft datasets  
  - Creating analytical views (e.g., total flight hours by aircraft group)

This layer produces structured, analytics-ready tables optimized for reporting.

---

### 4️⃣ Business Intelligence & Visualization
- Power BI connects to Azure SQL Database using DirectQuery for near real-time data access.
- The BI layer includes:
  - Data modeling and relationship management  
  - Power Query transformations  
  - DAX measures for KPIs and aggregations  

**Dashboards provide insights such as:**
- Flight activity metrics  
- Aircraft utilization  
- Ownership-based performance analysis  

---

## 🎯 Design Rationale
- **Automation:** Serverless functions and pipeline orchestration reduce manual intervention  
- **Scalability:** Cloud-native services support growing data volumes  
- **Centralized Storage:** Blob Storage and SQL Database ensure data consistency  
- **Real-Time Analytics:** DirectQuery enables up-to-date reporting  
- **Flexibility:** Python, SQL, and DAX support evolving business logic  

---

## 🧰 Technology Stack
- **Languages:** Python, SQL  
- **Cloud Platform:** Microsoft Azure  
  - Azure Blob Storage  
  - Azure Functions  
  - Azure Data Factory  
  - Azure SQL Database  
- **Analytics & BI:** Power BI (DirectQuery, DAX)



## 🚀 Key Outcomes
- Built a fully automated, cloud-based ETL pipeline  
- Implemented serverless data transformations  
- Designed analytics-ready SQL models  
- Delivered near real-time dashboards for operational insights  

This project demonstrates hands-on experience building production-grade data analytics solutions.
