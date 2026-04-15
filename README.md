![Python](https://img.shields.io/badge/Python-3.x-blue)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect-06b6d4)
![Pandas](https://img.shields.io/badge/Data-Pandas-black)
![SQLAlchemy](https://img.shields.io/badge/Database-SQLAlchemy-red)
![Neon PostgreSQL](https://img.shields.io/badge/Storage-Neon%20PostgreSQL-00E599?logo=postgresql&logoColor=white)

# Crypto Market ETL Pipeline

## Description

This project is a modular ETL pipeline that extracts cryptocurrency market data from the CoinGecko API, validates and transforms it, and loads it into a cloud-hosted PostgreSQL database (Neon) and Observer state with prefect UI Dashboard 

The pipeline is designed to simplify data access and enable fast, reliable analysis of market trends. By centralizing and structuring raw API data, it allows analytics teams to focus on reporting and insights rather than data preparation.

Built with **Python**, **Prefect**, **Pandas**, **Pydantic**, **HTTPX**, and **SQLAlchemy**.

---

## Overview

This project follows a standard ETL workflow:

1. **Extract** crypto market data from CoinGecko
2. **Save** raw API responses locally
3. **Validate** incoming records using Pydantic
4. **Transform** and clean the dataset with Pandas
5. **Load** the final data into Neon PostgresSql using SQLAlchemy core 
6. **Orchestrate** and automate the workflow with Prefect and Github Actions 


---

## Tech Stack

- **Python**
- **Prefect** – orchestration 
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
