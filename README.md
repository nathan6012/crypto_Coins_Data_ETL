![Python](https://img.shields.io/badge/Python-3.x-blue)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect-06b6d4)
![Pandas](https://img.shields.io/badge/Data-Pandas-black)
![SQLAlchemy](https://img.shields.io/badge/Database-SQLAlchemy-red)
![SQLite](https://img.shields.io/badge/Storage-SQLite-003b57)


# Crypto Market ETL Pipeline

A modular ETL pipeline that fetches cryptocurrency market data from the **CoinGecko API**, validates and transforms it, then loads it into a local **SQLite** database that can be qeried.

Built with **Python**, **Prefect**, **Pandas**, **Pydantic**, **HTTPX**, and **SQLAlchemy**.

---

## Overview

This project follows a standard ETL workflow:

1. **Extract** crypto market data from CoinGecko
2. **Save** raw API responses locally
3. **Validate** incoming records using Pydantic
4. **Transform** and clean the dataset with Pandas
5. **Load** the final data into SQLite using SQLAlchemy
6. **Orchestrate** and automate the workflow with Prefect Cloud

The goal of this project is to practice building a structured, production-style data pipeline rather than a one-off script.

---

## Tech Stack

- **Python**
- **Prefect** – orchestration and scheduling
- **Pandas** – transformation and cleaning
- **Pydantic** – schema validation
- **HTTPX** – API requests
- **SQLAlchemy** – database modeling and loading
- **SQLite** – local database storage
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
│   ├── processed/   # cleaned / transformed outputs
│   └── db/          # SQLite database
│
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── extract_data.py
│   ├── validate_data.py
│   ├── transform_data.py
│   ├── load_data.py
│   └── models.py
│
├── .env
├── .gitignore
├── requirements.txt
├── pyproject.toml
└── README.md

## How to Use

After setting up the project, run the pipeline to fetch the latest cryptocurrency market data from CoinGecko.

## Execution

This pipeline is designed to run automatically through **Prefect Cloud**.

It can also be triggered locally for development and testing:

```bash
with some changes on the flow in man.py 
then 
python -m src.main
