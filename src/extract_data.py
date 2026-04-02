import asyncio # used 
import httpx # used
import os # used 
from dotenv import load_dotenv #used 
from tenacity import retry, stop_after_attempt, wait_fixed #used 
from aiolimiter import AsyncLimiter # used 
#use = Used in the algorithm 

#___________________
load_dotenv() # Locate the .env 
limiter = AsyncLimiter(3, 1) # Limite Request rate
api_key = os.getenv("API_KEY") # gets the api key from .env 



async def extract_api_data():
  """Extracts Data from crypto about coins Api"""
  
  headers = {"x-cg-demo-api-key": api_key}
  params = {
    "vs_currency": "usd" }
  
 # Retry and backoff logic    
  @retry(stop=stop_after_attempt(3),
  wait = wait_fixed(2),reraise=True)
  async def extract(client): # ectract logic
    url = "https://api.coingecko.com/api/v3/coins/markets"
    
    async with limiter:
      response = await client.get(url,headers=headers,params=params)
      response.raise_for_status()
      fetch =  response.json()
     # print(fetch)
      return fetch
      
      
    # Extraction client   
  async with httpx.AsyncClient(timeout=30) as client:
    data = await extract(client)
      
    print("Data Extracted")
    # under the client logic 
    print(type(data))
    
    return data 




async def main():
  data = await extract_api_data()
  print(len(data)) 
  
if __name__ == "__main__":
 # print("Data collected")
  asyncio.run(main())
  
  
  
  
  

