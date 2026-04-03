import asyncio # used 
import httpx # used
import os # used 
from dotenv import load_dotenv #used
from aiolimiter import AsyncLimiter # used 
#use = Used in the algorithm 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

#___________________
load_dotenv() # Locate the .env 
# gets the api key from .env 



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



# Run main
async def main():
  data = await extract_api_data()
  print(len(data))
  
  
if __name__ == "__main__":
 # print("Data collected")
  asyncio.run(main())
#The Extract
  
  
  
  
  

