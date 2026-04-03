from prefect import flow, task,get_run_logger
import asyncio

from transform_data import transform_data
from load_data import load_data_to_db
from  validate_data import validate
from extract_data import extract_api_data
from save_raw_data import save_raw_data
from models import CryptoCoins


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))





@task(retries=3, log_prints=True)
async def extract_task():
  logger = get_run_logger()
  logger.info("Runing Extract")
  
  return await extract_api_data()
  


@task(log_prints=True) # saved raw file for task1
def save_task(raw):
  logger = get_run_logger()
  logger.info("Runing Save_raw_Data")
  
  save_raw_data(raw)
  
#Still gets from task1  
@task(retries=3, log_prints=True)
def validate_task(data,Model):
  logger = get_run_logger()
  logger.info("Runing validation")
  
  return validate(data,Model)   


@task(log_prints=True)
def transform_task(good, bad):
  logger = get_run_logger()
  logger.info("Runing Data Transformation")
  
  return transform_data(good, bad)

@task(retries=3, log_prints=True)
async def load_task(clean):
  logger = get_run_logger()
  logger.info("Runing Data Loading to Database")
  
  await load_data_to_db(clean)


# Main flow 

@flow(name="ETL_Crypto_Flow",log_prints=True)
async def master_flow_etl():
   # Step 1: Extract
    raw =  await extract_task()

    # Step 2: Save raw data
    save_task(raw)

    # Step 3: Validate
    good, bad = validate_task(raw,CryptoCoins)

    # Step 4: Transform
    clean = transform_task(good, bad)

    # Step 5: Load
    await load_task(clean)

  
#Deployment  
if __name__=="__main__":
  asyncio.run(master_flow_etl())
  
  
  

  
  
  
  
  


