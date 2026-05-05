![Python](https://img.shields.io/badge/Python-3.x-blue)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect-06b6d4)
![Pandas](https://img.shields.io/badge/Data-Pandas-black)
![SQLAlchemy](https://img.shields.io/badge/Database-SQLAlchemy-red)
![Cloudflare R2](https://img.shields.io/badge/Storage-Cloudflare%20R2-F38020?logo=cloudflare&logoColor=white)
![Neon PostgreSQL](https://img.shields.io/badge/Storage-Neon%20PostgreSQL-00E599?logo=postgresql&logoColor=white)

# Crypto Market ETL Pipeline

##📊 Overview
This project follows a standard ETL workflow:
Extract crypto market data from CoinGecko
Save raw API responses locally (data lake layer)
Validate incoming records using Pydantic
Transform and clean dataset using Pandas
Load structured data into Neon PostgreSQL using SQLAlchemy Core
Track state for incremental loading (avoid duplicate processing)
Orchestrate and automate the workflow using Prefect
CI/CD integration via GitHub Actions for reliability

##💼 Business Solution

This ETL pipeline solves a real-world data engineering problem: turning raw, inconsistent API data into reliable, structured, and query-ready financial datasets.
📉 Problem
Crypto market data from APIs like CoinGecko is:
Unstructured and nested
Frequently updated (high volatility)
Difficult to query directly for analytics
Prone to duplicates and inconsistent formats
This creates challenges for:
Data analysts
Trading dashboards
Financial reporting systems
Machine learning pipelines
##summery 
📊 Real-time analytics readiness for crypto market trends
📉 Reduced data redundancy through incremental ETL
⚡ Faster decision-making for traders and analysts
🧠 Reliable data foundation for forecasting models
☁️ Scalable cloud-based data storage (Neon PostgreSQL)

---

## Tech Stack

- **Python**
- **Prefect** – orchestration 
- ** R2(s3)** -  Datalake 
- **Pandas** – transformation and cleaning
- **Pydantic** – schema validation
- **HTTPX** – API requests
- **SQLAlchemy** – database modeling and loading
- **PostgresSql** – Online database storage
- **GitHub Actions** – CI/CD workflow automation

---

## Project Structure

```bash
project/
│
├── .github/
│   └── workflows/
│       └── pipeline.yml
│
├── data/
│   ├── raw/         # raw API responses (json/csv)
│ 
│ 
│
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── extract_data.py
│   ├── validate_data.py
│   ├── transform_data.py
│   ├── load_data.py
│   └── models.py
│____tests/ # to be added in updates order 

├── .env
├── .gitignore
├── requirements.txt
├── pyproject.toml
└── README.md

## How to Use

collect Credentials for Prefect,Slack notifcations ,api key and NeonPostgresSql database url

# run in IDE cli/terminal
setup venv: activate it 
copy teh repo  from github 
git clone https://github.com/nathan6012/crypto_Coins_Data_ETL.git
set all needed credentials 
cd to root -+folder 
run python src/main.py 

#prefect setup in linux

to use prefect cloud must have api key ,workpool and api url ready and active 

export PREFECT_API_KEY="your_api_key_here"
export PREFECT_API_URL="https://prefect.cloud"
then run : prefect config view 

#windows  setup after login in cloud 
# Set your API Key
[System.Environment]::SetEnvironmentVariable("PREFECT_API_KEY", "your_api_key_here", "User")

# Set your API URL
[System.Environment]::SetEnvironmentVariable("PREFECT_API_URL", "https://prefect.cloud", "User")



add the database and api add them to .env file , run the pipeline:
  python main.py in root folder 
cryptocurrency market data from CoinGeck
## Execution

This pipeline is designed to run automatically through **Prefect Github Actions and Observer on prefect cloud UI or github action logs  **.


Note: to run in github actions add Slack webhook url to /Env .secrets
## Future Improvements cloud S3/
Tests 
