
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv
from prefect.schedules import Interval
from datetime import timedelta, datetime
from prefect.blocks.notifications import SlackWebhook
from prefect.states import State

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



def load_slack_block():
  try:
    return SlackWebhook.load("prefect-alerts-system01")
  except Exception as e:
    print(f"Slack block not available: {e}")
    return None


def slack_on_completion(flow, flow_run, state: State):
  slack_webhook_block = load_slack_block()

  if not slack_webhook_block:
    print("Slack notification skipped.")
    return

  message = f":white_check_mark: Flow `{flow_run.name}` completed successfully!"

  try:
    slack_webhook_block.notify(message)
  except Exception as e:
    print(f"Failed to send Slack success message: {e}")




@task(retries=3, log_prints=True)
async def extract_task():
  
  return await extract_api_data()


@task(log_prints=True) # saved raw file for task1
def save_task(raw):
  
  save_raw_data(raw)
  
#Still gets from task1  
@task(retries=3, log_prints=True)
def validate_task(data,Model):
  
  return validate(data,Model)   


@task(log_prints=True)
def transform_task(good, bad):
  
  return transform_data(good, bad)

@task(retries=3, log_prints=True)
def load_task(clean):
  
  load_data_to_db(clean)


# Main flow 

@flow(name="ETL_Crypto_Flow",
on_completion=[slack_on_completion],
log_prints=True)
def master_flow_etl():
   # Step 1: Extract
    raw = extract_task()

    # Step 2: Save raw data
    save_task(raw)

    # Step 3: Validate
    good, bad = validate_task(raw,CryptoCoins)

    # Step 4: Transform
    clean = transform_task(good, bad)

    # Step 5: Load
    load_task(clean)

  
#Deployment  
if __name__=="__main__":
  master_flow_etl.from_source(
        source="https://github.com/nathan6012/crypto_Coins_Data_ETL.git",
        
  entrypoint="src/main.py:master_flow_etl"
   ).deploy(
      name="etl_github-deployment",
      work_pool_name="My_etl_system",
      job_variables = { "pip_packages":[
        "pandas","httpx","python-dotenv",
        "pydantic",
        "sqlalchemy",
        "aiolimiter" ]
        
      },
      interval=timedelta(hours=2)
    )


  
  
  
  
  


