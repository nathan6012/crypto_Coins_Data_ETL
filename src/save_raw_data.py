import sys
import os
import logging 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
from pathlib import Path
from datetime import datetime


logging.getLogger().setLevel(logging.INFO)



def save_raw_data(data):
  """Saves Raw Json file for Backfill just in case"""
  # Now access the root sub_folder and save 
  dir_url = Path(__file__).resolve().parent
  root_dir = dir_url.parent
  sub_folder = root_dir/"data"

  sub_folder.mkdir(parents=True, exist_ok=True)

  
  ts = datetime.now().strftime("%Y%m%d_%H%M%S")
  
  file_locate  = f"raw_data_{ts}.json"
  
  file= sub_folder/file_locate
  
  with open(file,"w") as file:
    json.dump(data,file,indent=3)
    logging.info(f"Raw api data saved")
    
  
def main():
  save_raw_data()
  
if __name__=="__main__":
  main()
  
  