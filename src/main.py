import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))




from prefect import flow, task,get_run_logger
import asyncio

import logging 
from src.transform import transform_data
from src.load_data import run_incremental
from src.validate_data import validate
from src.extract_data import extract_api_data
from src.save_raw_data import save_raw_data
from src.models import CryptoCoins


from src.system_manager import setup_logger
logging, log_file = setup_logger()


@task(retries=3, log_prints=True)
async def extract_task():
  logging.info("Runing Extract")
  
  return await extract_api_data()
  


@task(log_prints=True) # saved raw file for task1
def save_task(raw):
  #logger = get_run_logger()
  logging.info("Runing Save_raw_Data")
  
  save_raw_data(raw)
  
#Still gets from task1  
@task(retries=3, log_prints=True)
def validate_task(data,Model):
 # logger = get_run_logger()
  logging.info("Runing validation")
  
  return validate(data,Model)   


@task(log_prints=True)
def transform_task(good, bad):
  #logger = get_run_logger()
  logging.info("Runing Data Transformation")
  
  return transform_data(good, bad)

@task(retries=3, log_prints=True)
async def load_task(clean):
  #logger = get_run_logger()
  logging.info("Runing Data Loading to Database")
  
  await run_incremental(clean)


# Main flow 

@flow(name="ETL_Crypto_Flow",log_prints=True)
async def master_flow_etl():
   # Step 1: Extract
  raw =  await extract_task()
  if raw:

    # Step 2: Save raw data
    save_task(raw)

    # Step 3: Validate
    good, bad = validate_task(raw,CryptoCoins)

    # Step 4: Transform
    clean = transform_task(good, bad)

    # Step 5: Load
    logging.info("Staging to Database")
    
    await run_incremental(clean)
    
    logging.info("Data Pipeline Run Successfully")
  else:
    logging.info("No Data to processs")

  
if __name__=="__main__":
  asyncio.run(master_flow_etl())
  
  
  

  
  
  
  
  


