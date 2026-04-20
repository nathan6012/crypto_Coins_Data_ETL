 import pandas as pd
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
logging.getLogger().setLevel(logging.INFO)


# -----------------------------
# HELPERS
# -----------------------------
def convert_dates(df, date_cols):
  for col in date_cols:
    if col in df.columns:
      df[col] = (
                pd.to_datetime(df[col], errors="coerce", utc=True)
                .dt.tz_localize(None)
            )
  return df


def rename_roi_columns(df):
  return df.rename(columns={
        "data.roi.currency": "roi_currency",
        "data.roi.times": "roi_times",
        "data.roi.percentage": "roi_percentage"
    })


def cast_numeric(df, float_cols, int_cols):
  for col in float_cols:
    if col in df.columns:
      df[col] = pd.to_numeric(df[col], errors="coerce")

  for col in int_cols:
    if col in df.columns:
      df[col] = pd.to_numeric(df[col], errors="coerce", downcast="integer")

  return df


def cast_strings(df, str_cols):
  for col in str_cols:
    if col in df.columns:
      df[col] = df[col].astype(str).replace("nan", None)
  return df


def clean_nulls(records):
  cleaned = []
  for row in records:
    cleaned.append({
            key: (None if pd.isna(value) else value)
            for key, value in row.items()
        })
  return cleaned


# -----------------------------
# MAIN TRANSFORM
# -----------------------------
def transform_data(good, bad):

  df = pd.json_normalize(good)
  df1 = pd.json_normalize(bad)

  print(len(df))
  print(len(df1))

  pd.set_option("display.max_columns", None)
  pd.set_option("display.max_rows", None)

  date_cols = [
        "data.ath_date",
        "data.atl_date",
        "data.last_updated"
    ]

    # -----------------------------
    # CLEAN BOTH DATAFRAMES
    # -----------------------------
  df = convert_dates(df, date_cols)
  df1 = convert_dates(df1, date_cols)

  df = rename_roi_columns(df)
  df1 = rename_roi_columns(df1)

  if "errors" in df1.columns:
    df1 = df1.drop(columns=["errors"])

    # -----------------------------
    # COMBINE
    # -----------------------------
  combined = pd.concat([df, df1])

  data = (
        combined
        .set_index("idx")
        .sort_index()
        .drop_duplicates()
        .reset_index()
    )

    # -----------------------------
    # DROP / RENAME
    # -----------------------------
  data = data.drop(columns=["idx", "data.image"], errors="ignore")
  data.columns = data.columns.str.replace("data.", "", regex=False)

    # -----------------------------
    # TYPE CASTING
    # -----------------------------
  float_cols = [
        "current_price", "total_volume", "high_24h", "low_24h",
        "price_change_24h", "price_change_percentage_24h",
        "market_cap_change_24h", "market_cap_change_percentage_24h",
        "circulating_supply", "total_supply", "max_supply",
        "ath", "ath_change_percentage", "atl", "atl_change_percentage",
        "roi_times", "roi_percentage"
    ]

  int_cols = ["market_cap", "market_cap_rank", "fully_diluted_valuation"]

  str_cols = ["id", "symbol", "name", "roi_currency"]

  data = cast_numeric(data, float_cols, int_cols)
    data = cast_strings(data, str_cols)

    # -----------------------------
    # FINAL OUTPUT
    # -----------------------------
  data_records = data.to_dict(orient="records")
  print(type(data_records))

  cleaned_records = clean_nulls(data_records)



 # print(data.columns)
  
  print(len(cleaned_records))
  
  logging.info("Data Transformed")
 # print(type(cleaned_records))

  
  return cleaned_records
  
#@taksk(name="Main_Clean")  

  
  
  
  
  



