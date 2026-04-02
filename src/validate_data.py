from pydantic import BaseModel,ValidationError
from models import CryptoCoins

from prefect import task,get_run_logger 
from pathlib import Path
import json

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))



  
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
      
      
  if valid_fields:
    print("Good data",len(valid_fields))
    
    dir_url = Path(__file__).resolve().parent
    root_dir = dir_url.parent
    sub_folder = root_dir/"data"
  
    file_1 = sub_folder/"valid.json"
    
    with open(file_1,"w") as f:
      json.dump(valid_fields,f,indent=1)
  else:
    print("Bad Data only")


  if invalid_fields:
    print("Bad data",len(invalid_fields))
    
    dir_url = Path(__file__).resolve().parent
    root_dir = dir_url.parent
    sub_folder = root_dir/"data"
    
    file = sub_folder/"invalid.json"
    
    with open(file,"w") as f:
      json.dump(invalid_fields,f,indent=1)
  else:
    print("Clean Data Only ")
  print("Data validated")
  
  
  
  return  valid_fields,invalid_fields
  



def main():

  good,bad = validate()
  
if __name__ == "__main__":
  main()
  
  