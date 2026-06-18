import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text,MetaData

load_dotenv()

db_url = os.getenv("DATABASE_URL_2")

if not db_url:
    raise ValueError("DATABASE_URL not found")

db_url = db_url.strip()

engine = create_engine(db_url, echo=False)



def get_conn():
    return engine.connect()