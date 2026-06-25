import os
import json
import logging

import boto3
import pandas as pd

from datetime import datetime, UTC
from dotenv import load_dotenv


logging.getLogger().setLevel(logging.INFO)

load_dotenv()


BUCKET_NAME = "nathan-elt-buck"


def save_raw_data(data):
    """
    Save raw market data to S3/R2 datalake.
    """

    if not data:
        logging.warning("Data records are empty")
        return


    endpoint = os.getenv("endpoint_url")
    access_key_id = os.getenv("access_key_id")
    secret_access_key = os.getenv("secret_key")


    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto"
    )


    try:
        # Convert records to dataframe
        df = pd.DataFrame(data)


        # Convert dataframe to JSON lines
        records = df.to_json(
            orient="records",
            lines=True
        )


        logging.info(
            f"Records prepared: {len(df)}"
        )


        now = datetime.now(UTC)


        key = (
            f"crypto/json/"
            f"{now:%Y/%m/%d/%H}/"
            f"crypto_markets.json"
        )


        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=records.encode("utf-8"),
            ContentType="application/json"
        )


        logging.info(
            "Data staged to Data Lake successfully"
        )


    except Exception as e:

        logging.error(
            f"Failed to load data to Data Lake: {e}"
        )

        # Important for ETL monitoring
        raise RuntimeError(
            "Raw data ingestion failed"
        ) from e