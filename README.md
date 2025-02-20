### ** Project Title: Transport Accident Analysis & Correlation with Weather in London**  

#### ** Project Overview**
This project aims to analyze **road traffic accidents in London** using the **TfL AccidentStats API** and correlate them with external factors such as **weather conditions, time of day, and location-based risk factors**. By leveraging **data engineering best practices**, this project will ingest, transform, and visualize accident data to identify key insights into **accident severity, high-risk areas, and trends over time**.  

#### **📊 Key Insights & Objectives**
- Identify **London’s most accident-prone locations** and peak accident times.  
- Analyze how **weather conditions (rain, fog, wind) influence accident severity**.  
- Correlate **public transport density and traffic congestion** with accidents.  
- Provide **interactive dashboards** for stakeholders to explore accident patterns.  

#### **🛠 Tech Stack & Architecture**
- **Infrastructure as Code (IaC):** **Terraform** for provisioning **GCP resources**.  
- **Data Ingestion:** **DLT** to extract accident/weather data & store in **GCS bucket**.  
- **Data Processing:** **CloudSQL (PostgreSQL) + dbt** for transformation.  
- **Visualization:** **Streamlit on Cloud Run** for an interactive dashboard.  
- **Orchestration:** **VM for dbt & scheduled ingestion jobs**.  

#### ** Data Sources**
- **TfL AccidentStats API** (2005-2019) → Accident records, severity, locations.  
- **OpenWeather API** → Historical weather data (rain, fog, wind).  
- **Optional:** Google Traffic API → Real-time congestion data.  

#### ** Expected Outcomes**
By the end of this project, we will have a **fully automated data pipeline** that provides **real-time insights into London's accident trends**, enabling better **safety planning, policy-making, and risk assessment** for commuters and authorities.  

---
