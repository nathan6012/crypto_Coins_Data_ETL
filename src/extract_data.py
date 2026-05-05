import sys
import os 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import asyncio # used 
import httpx # used
import logging 
import os # used 
from dotenv import load_dotenv #used
from aiolimiter import AsyncLimiter # used 
#use = Used in the algorithm 

from src.system_manager import setup_logger
logging, log_file = setup_logger()


#___________________
load_dotenv() # Locate the .env 
# gets the api key from .env 
#logging.getLogger().setLevel(logging.INFO)



async def extract_api_data():
  """Extracts Data from crypto about coins Api"""
  limiter = AsyncLimiter(3, 1)
  api_key = os.getenv("API_KEY") 
  
  
  headers = {"x-cg-demo-api-key": api_key}
  params = {
    "vs_currency": "usd" }
 # Retry and backoff logic    
  url = "https://api.coingecko.com/api/v3/coins/markets"
  async with httpx.AsyncClient(timeout=30.0) as client:
    async with limiter:
      response = await client.get(url, headers=headers, params=params)
      response.raise_for_status()
      data = response.json()

    return data 

  
  
  
  

