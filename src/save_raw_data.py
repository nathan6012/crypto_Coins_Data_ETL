import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json 


from datetime import datetime
import boto3
import logging 
from io import BytesIO
import pandas as pd
from dotenv import load_dotenv 


logging.getLogger().setLevel(logging.INFO)
load_dotenv()


def save_raw_data(data):
  
  """S3/R2 Datalake"""
  endpoint = os.getenv("endpoint_url")
  access_key_id = os.getenv("access_key_id")
  secret_access_key = os.getenv("secret_key")
  
  
  bucket = "nathan-elt-buck"
  
  s3 = boto3.client(
   "s3",
  endpoint_url=endpoint,
  aws_access_key_id=access_key_id,
  aws_secret_access_key=secret_access_key,
  region_name="auto"
    )
  
  if data:
    df = pd.DataFrame(data)
  
    records = df.to_json(orient="records", lines=True)
    logging.info(f"data:{len(records)}")
    try:
      now = datetime.utcnow()
      
      s3.put_object(
        Bucket=bucket,
        Key=f"crypto/json/{now:%Y/%m/%d/%H}crypto_markets.json",
        Body=records.encode("utf-8"),
        ContentType="application/json")
        
      logging.info("Data staged to Data lake")
    except Exception as e:
      logging.warning(f" Failed to load to Datalake: {e}")
  else:
    logging.warning("Data Records are Empty")
    
  
  

  


  