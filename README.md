
# 📊 Crypto Market ETL Pipeline + Streamlit Dashboard

![Python](https://img.shields.io/badge/Python-3.x-blue)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect-06b6d4)
![Pandas](https://img.shields.io/badge/Data-Pandas-black)
![SQLAlchemy](https://img.shields.io/badge/Database-SQLAlchemy-red)
![PostgreSQL](https://img.shields.io/badge/Database-Neon%20PostgreSQL-00E599?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Cloudflare R2](https://img.shields.io/badge/Storage-Cloudflare%20R2-F38020?logo=cloudflare&logoColor=white)

---

## 📌 Overview

This project is an **end-to-end Data Engineering pipeline** that collects cryptocurrency market data, processes it through an ETL workflow, stores it in a cloud PostgreSQL database, and visualizes insights using an interactive **Streamlit dashboard**.

The system follows a modern data architecture:
- Extract data from CoinGecko API
- Store raw data (data lake layer)
- Validate using Pydantic
- Transform using Pandas
- Load into Neon PostgreSQL
- Orchestrate using Prefect
- Visualize using Streamlit dashboard

---

## 💼 Problem Statement

Crypto market data from APIs is:
- Unstructured and nested
- Highly volatile (changes every few seconds)
- Not directly analytics-ready
- Prone to duplicates and inconsistent formats

### ❌ This creates problems for:
- Data analysts
- Traders
- Financial dashboards
- Machine learning models

---

## ✅ Solution

This ETL pipeline solves these problems by:

- Structuring raw crypto API data into clean tables
- Ensuring data consistency using validation rules
- Removing duplicates through incremental loading
- Storing clean data in PostgreSQL (Neon Cloud)
- Providing real-time insights through a Streamlit dashboard with Clean Data from centralized location 

---

## 📊 Business Impact

- 📈 Real-time crypto analytics dashboard
- ⚡ Faster decision-making for market analysis
- 🧠 Clean dataset for forecasting models
- ☁️ Scalable cloud-based data storage
- 🔁 Automated ETL with Prefect orchestration

---

## 🧰 Tech Stack

- **Python** – core language
- **Prefect** – workflow orchestration
- **Pandas** – data transformation
- **Pydantic** – validation layer
- **HTTPX** – API requests
- **SQLAlchemy** – database operations
- **PostgreSQL (Neon)** – cloud database
- **Streamlit** – interactive dashboard
- **Cloudflare R2** – raw data storage
- **GitHub Actions** – CI/CD automation

---

## 📁 Project Structure

```bash
project/
│
├── dashboards/        # Streamlit dashboard
│   ├── app.py
│   ├── charts.py
│   ├── kpis.py
│   └── queries.py
│
├── src/               # ETL pipeline
│   ├── main.py
│   ├── extract_data.py
│   ├── transform.py
│   ├── validate_data.py
│   ├── load_data.py
│   └── models.py
│
├── shared/            # shared utilities (DB, helpers)
├── sql/               # database schema
├── data/              # raw data storage
├── tests/             # unit tests
│
├── .github/workflows/ # CI/CD pipeline
├── requirements.txt
└── README.md

## local run 
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows


git clone https://github.com/your-username/crypto-etl.git
cd crypto-etl


pip install -r requirements.txt

python src/main.py

python -m streamlit run dashboards/app.py
streamlit run dashboards/app.py

