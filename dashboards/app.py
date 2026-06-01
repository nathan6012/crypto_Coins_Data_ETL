import streamlit as st
import pandas as pd
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import os 
from loadenv import load_dotenv


load_dotenv()

db_url = os.getenv("DATABASE_URL").strip()
engine = create_async_engine(db_url, echo=False)
metadata = MetaData()

async def fetch_kpis():
  pass


async def fetch_monthly_trend():
  pass 



async def fetch_sample():
  pass

  
  
  
  
  




















































