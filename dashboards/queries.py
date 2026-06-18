import streamlit as st
import pandas as pd


from sqlalchemy import text


def get_market_data(conn):
    result = conn.execute(
        text("""
        SELECT
    name,
    market_cap,
    price_change_percentage_24h
FROM crypto_markets
        """)
    )

    return result.fetchall()
    
    

def get_crypto_details(conn):

    result =  conn.execute(
        text("""
        SELECT
            name,
            symbol,
            current_price,
            market_cap,
            market_cap_rank,
            price_change_percentage_24h,
            total_volume
        FROM crypto_markets
        ORDER BY market_cap DESC
        LIMIT 5
        """)
    )

    return result.fetchall()