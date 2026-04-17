import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))



from pydantic import BaseModel,ValidationError
from models import CryptoCoins

from prefect import task,get_run_logger 
from pathlib import Path
import json
import logging 


logging.getLogger().setLevel(logging.INFO)
  
def validate(data,Model):
  valid_fields = []
  invalid_fields = []
  
  for idx,records in enumerate(data):
    try:
    
      valid  = Model(**records)
      valid_fields.append({
       "idx":idx,
       "data":valid.model_dump(mode="json")
      })
    except ValidationError as e:
      invalid_fields.append({
        "idx": idx,
        "data":records,
        "errors":e.errors()
      })
      
  
  
  
  #Not necessary  
  #Just to see the state of data 
  if valid_fields:
    logging.info("Good data")
    print(len(valid_fields))
    
    dir_url = Path(__file__).resolve().parent
    root_dir = dir_url.parent
    sub_folder = root_dir/"data"
  
    file_1 = sub_folder/"valid.json"
    
    with open(file_1,"w") as f:
      json.dump(valid_fields,f,indent=1)
  else:
    logging.info("Bad Data only")


  if invalid_fields:
    logging.info("Bad data")
    print(len(invalid_fields))
    
    dir_url = Path(__file__).resolve().parent
    root_dir = dir_url.parent
    sub_folder = root_dir/"data"
    
    file = sub_folder/"invalid.json"
    
    with open(file,"w") as f:
      json.dump(invalid_fields,f,indent=1)
  else:
    logging.info("Clean Data Only ")
    
  
  
  #Must Return/yield for the next task   
  logging.info("Data validated")
  return  valid_fields,invalid_fields
  



  
  