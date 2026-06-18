#make docker file 
FROM python:3.13-slim

WORKDIR /app

# Install system deps (important for pandas/psycopg builds)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Streamlit port
EXPOSE 8501

# Correct Streamlit run command
CMD ["python", "-m", "streamlit", "run", "dashboards/app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501"]