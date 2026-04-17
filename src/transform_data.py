import pandas as pd
import json 
from pandas import json_normalize
import logging 

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.getLogger().setLevel(logging.INFO)





def transform_data(good,bad):
 
  df = pd.json_normalize(good)
  print(len(df))
  
  df1 = pd.json_normalize(bad)
  print(len(df1))

  #This is the valid data
  #display settings
  pd.set_option("display.max_columns", None)
  pd.set_option("display.max_rows", None)
  df.style.set_properties(**{"text-align": "center"})
  
  
  df["data.ath_date"] = pd.to_datetime(df["data.ath_date"], errors="coerce", utc=True).dt.tz_localize(None)
  
  df["data.atl_date"] = pd.to_datetime(df["data.atl_date"], errors="coerce", utc=True).dt.tz_localize(None)
  
  df["data.last_updated"] = pd.to_datetime(df["data.last_updated"], errors="coerce", utc=True).dt.tz_localize(None)
  
  
  df = df.rename(columns={
    "data.roi.currency": "roi_currency",
    "data.roi.times": "roi_times",
    "data.roi.percentage": "roi_percentage"
})  
  
  
    #print(df1.columns)
 # print(df1["errors"])
  
#  print(df1.dtypes)

  df1["data.ath_date"] = pd.to_datetime(df1["data.ath_date"], errors="coerce", utc=True).dt.tz_localize(None)
  
  df1["data.atl_date"] = pd.to_datetime(df1["data.atl_date"], errors="coerce", utc=True).dt.tz_localize(None)
  
  df1["data.last_updated"] = pd.to_datetime(df1["data.last_updated"], errors="coerce", utc=True).dt.tz_localize(None)
  
  df1 = df1.drop(columns=["errors"])
  

    
  df1 = df1.rename(columns={
    "data.roi.currency": "roi_currency",
    "data.roi.times": "roi_times",
    "data.roi.percentage": "roi_percentage"})  
  

  
  
  
#  print(df1["data.roi.currency"])
 # print(df1.dtypes)
  combined = pd.concat([df,df1])
  data = combined.set_index("idx").sort_index()
  data = data.drop_duplicates()
  data = data.reset_index()
  #print("Done")
  #print(data.dtypes)
  #print(data.dtypes)
  data = data.drop(columns=["idx"])
  data = data.drop(columns=["data.image"])
  
  data.columns=data.columns.str.replace("data.","",regex=False)
  #data.columns=data.columns.str.replace("_"," ",regex=False)
 # data.columns=data.columns.str.upper()
  
 # print(data.dtypes)
  float_cols = [
        "current_price", "total_volume", "high_24h", "low_24h",
        "price_change_24h", "price_change_percentage_24h",
        "market_cap_change_24h", "market_cap_change_percentage_24h",
        "circulating_supply", "total_supply", "max_supply",
        "ath", "ath_change_percentage", "atl", "atl_change_percentage",
        "roi_times", "roi_percentage"
    ]
  for col in float_cols:
    if col in data.columns:
      data[col] = pd.to_numeric(data[col], errors="coerce")  
      
      
      
  int_cols = ["market_cap", "market_cap_rank", "fully_diluted_valuation"]  
  for col in int_cols:
    if col in data.columns:
      data[col] = pd.to_numeric(data[col], errors="coerce", downcast="integer")
      
      
  str_cols = ["id", "symbol", "name", "roi_currency"] 
  for col in str_cols:
    if col in data.columns:
      data[col] = data[col].astype(str).replace("nan", None)
  
 # data = data.where(data.notnull(data), None)
    
      
      
  
# Ensure roi_currency is a string, replace NaNs or numbers with empty string
  
  
  data_records = data.to_dict(orient="records")
  print(type(data_records))
  
  cleaned_records = []
  for row in data_records:
    cleaned_row = {
        key: (None if pd.isna(value) else value)
        for key, value in row.items()
    }
    cleaned_records.append(cleaned_row)
  
  

 # print(data.columns)
  
  print(len(cleaned_records))
  
  logging.info("Data Transformed")
 # print(type(cleaned_records))
  
  
  
  
  #Test 
  #File used for testing 
  
  return cleaned_records
  
#@taksk(name="Main_Clean")  

  
  
  
  
  



