from transform_data import transform_data
from load_data import load_data_to_db
from  validate_data import validate
from extract_data import extract_api_data
from save_raw_data import save_raw_data
from models import CryptoCoins
import asyncio

def main():
  raw = asyncio.run(extract_api_data())
  
  save_raw_data(raw)
  
  good,bad =  validate(raw,CryptoCoins)
  
  clean = transform_data(good,bad)
  
  load_data_to_db(clean)
  
  
if __name__=="__main__":
  main()