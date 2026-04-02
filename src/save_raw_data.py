import json
from pathlib import Path


def save_raw_data(data):
  """Saves Raw Json file for Backfill just in case"""
  # Now access the root sub_folder and save 
  dir_url = Path(__file__).resolve().parent
  root_dir = dir_url.parent
  sub_folder = root_dir/"data"

  sub_folder.mkdir(parents=True, exist_ok=True)
  data_path  = sub_folder
  
  with open(data_path/"raw_data.json","w") as file:
    json.dump(data,file,indent=3)
    print("Raw api data saved")
    
  
def main():
  save_raw_data()
  
if __name__=="__main__":
  main()
  
  