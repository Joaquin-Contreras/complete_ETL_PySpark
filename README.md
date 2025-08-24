# complete_ETL_PySpark

## Project Overview

This project extracts, transforms, and loads TMDB movie data inot a MySQL data warehouse using PySpark. It also provides an API for querying processed data.

Goals and scope:
- Data processing, ETL pipeline, schema design, API exposure.

Project diagram:
<img width="1457" height="598" alt="image" src="https://github.com/user-attachments/assets/56479228-bc20-41d9-9da3-3d1954e496da" />


## Database Design

For this project I organize the dabatase in a *Star Schema* (fact and dimension tables).

You can view the complete database schema here: [Database Schema PDF](database_schema.pdf)

## File structure & modules
```bash
project/
├─ data/raw/         # raw JSON files
├─ ingest/           # intake.py
├─ spark_jobs/       # PySpark ETL scripts
├─ db/               # init SQL scripts
├─ utils/            # some functions and modules
├─ Dockerfile
├─ docker-compose.yml
└─ README.md

```

---

## Requirements  
- Python 3.10+  
- Docker and Docker Compose installed  
- Git  

---

## Installation & Setup  

### Clone the repository  
```bash
git clone https://github.com/Joaquin-Contreras/complete_ETL_PySpark.git
cd complete_ETL_PySpark
python -m venv venv
source venv/bin/activate      # Linux / Mac
venv\Scripts\activate         # Windows
```
### Install dependencies
```bash
pip install -r requirements.txt
```


## Data Ingestion
Tihs project uses the TMDB API to fetch raw *.json* files.
<br>
The API key is already included in the ingestion script for simlicity.

Run the ingestion
```bash
python -m spark_jobs.ingest.intake
```
Thi will generate raw data inside:
```bash
data/raw/
```

## Processing & Loeading with Spark + MySQL

Once you have *.json* files in *data/raw/*, start the services with Docker:
```bash
docker-compose up --build
```
This will
- Launch a PySpark container to process raw *.json* files.
- Load the transformed data into a Star Schema inside a MySQL database.
--- 

## Database Access

You can connect to MySQL inside the container with:
```bash
docker exec -it mysql-server mysql -u root -p
```
Default credentials
- User: root
- Password: root
- Database: tmdb_database

## NOTES:

- This project uses a public API key included in the repo to simplify onboarding.
- Data is stored in a relational database designed for analytical queries.
- The main goal is to showcase a full ETL pipelin: ingestion -> transformation -> loading.

## Purpose

This project demonstrates:
- Data engineering skills (ETL pipelines, PySpark, Docker, MySQL)
- Database Modeling (Star Schema for analytics)
- Practical Data Pipeline Deployment (end-to-end reproducible workflow)



















