import pandas as pd
import json 
from pandas import json_normalize

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))




def transform_data(good,bad):
 
  df = pd.json_normalize(good)
  df1 = pd.json_normalize(bad)

  
  
  #This is the valid data
  #display settings
  pd.set_option("display.max_columns", None)
  pd.set_option("display.max_rows", None)
  df.style.set_properties(**{"text-align": "center"})
  
  
  df["data.ath_date"] = pd.to_datetime(df["data.ath_date"], errors="coerce", utc=True).dt.tz_localize(None)
  
  df["data.atl_date"] = pd.to_datetime(df["data.atl_date"], errors="coerce", utc=True).dt.tz_localize(None)
  
  df["data.last_updated"] = pd.to_datetime(df["data.last_updated"], errors="coerce", utc=True).dt.tz_localize(None)
  
  
  df=df.astype({
  "data.current_price": "float32",
  "data.high_24h": "float32",
  "data.low_24h": "float32",
  
  "data.price_change_24h": "float32",
  "data.price_change_percentage_24h": "float32",
  "data.market_cap_change_percentage_24h": "float32",
  
  "data.circulating_supply": "float32",
  "data.total_supply": "float32",
  "data.max_supply": "float32",
  
  "data.ath":"float32",
  "data.ath_change_percentage":"float32",
  
  "data.atl":"float32",
  "data.atl_change_percentage":"float32",
  
  "data.roi.times":"float32",
  "data.roi.currency":"str",
  "data.roi.percentage":"float32"
    })
    
#  df["market_cap_change_24h"] = df["market_cap_change_24h"].apply(lambda x: f"{x:,}")  

  fill_value = 0
  df = df.fillna(fill_value)
 # print(df.dtypes)
  
 # print(df["data.id"])
 # print("Saved")
 
  df = df.drop(columns=["data.id"])
 # df["data.symbol"]=df["data.symbol"].str.upper()
  
  
#  print(df["data.roi.currency"])
 # print(df.dtypes)
  
 # df.to_csv("test_csv.csv",index=False)
 
 # print(df["data.last_updated"])
  
  
  
  print()# We can have some separation of output 
  print()
  
  
    #print(df1.columns)
 # print(df1["errors"])
  df1 = df1.drop(columns=["errors"])
#  print(df1.dtypes)

  df1["data.ath_date"] = pd.to_datetime(df["data.ath_date"], errors="coerce", utc=True).dt.tz_localize(None)
  df1["data.atl_date"] = pd.to_datetime(df["data.atl_date"], errors="coerce", utc=True).dt.tz_localize(None)
  df1["data.last_updated"] = pd.to_datetime(df["data.last_updated"], errors="coerce", utc=True).dt.tz_localize(None)
  
  
  
  df1["data.total_volume"] = df1["data.total_volume"].astype("int32")
  
  df1=df1.astype({
  "data.current_price": "float32",
  "data.high_24h": "float32",
  "data.low_24h": "float32",
  
  "data.price_change_24h": "float32",
  "data.price_change_percentage_24h": "float32",
  "data.market_cap_change_percentage_24h": "float32",
  
  "data.circulating_supply": "float32",
  "data.total_supply": "float32",
  "data.max_supply": "float32",
  
  "data.ath":"float32",
  "data.ath_change_percentage":"float32",
  
  "data.atl":"float32",
  "data.atl_change_percentage":"float32",
  
  "data.roi.times":"float32",
  "data.roi.currency":"str",
  "data.roi.percentage":"float32"
    })
  
  fill_value = 0
  df1= df1.fillna(fill_value)
  
  df1 = df1.drop(columns=["data.id"])
  df1["data.symbol"]=df1["data.symbol"].str.upper()
  df1["data.market_cap_change_24h"]=df1["data.market_cap_change_24h"].astype("int64")
  
  
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
  data = data.rename(columns={
  "roi.times": "roi_times",
  "roi.currency": "roi_currency",
  "roi.percentage": "roi_percentage"
  }) # Better for Db 

  data['roi_currency'] = data['roi_currency'].astype(str).replace({'nan': None})
# Ensure roi_currency is a string, replace NaNs or numbers with empty string
  
  
  data_records = data.to_dict(orient="records")
  print(type(data_records))
  df1['roi_currency'] = df1['roi_currency'].astype(str).replace({'nan': None})
  

 # print(data.columns)
  
  print("Data Transformed")
  
  #Test 
  #File used for testing 
  
  return data_records
  
#@taksk(name="Main_Clean")  
def main():
  clean = transform_data()

if __name__=="__main__":
  main()
  
  
  
  
  



