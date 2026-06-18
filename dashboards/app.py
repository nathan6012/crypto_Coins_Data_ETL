import streamlit as st
import pandas as pd
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from kpis import (
    total_coins,
    total_market_cap,
    avg_price,
    top_coin,
    top_gainer,
    top_loser
)

from queries import get_market_data, get_crypto_details

from charts import (
    bar_market_cap_chart,
    line_price_change_chart,
    pie_market_share_chart
)

# -----------------------------
# DATABASE
# -----------------------------
load_dotenv()

db_url = os.getenv("DATABASE_URL_2")

if not db_url:
    raise ValueError("DATABASE_URL not found")

db_url = db_url.strip()

engine = create_engine(db_url, echo=False)

def get_conn():
    return engine.connect()

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="ETL Dashboard",
    layout="wide"
)

st.title("Crypto ETL Analytics Dashboard")
st.write("Crypto Coins Data growth over time ()")

# -----------------------------
# KPI SECTION
# -----------------------------
with get_conn() as conn:
    kpis = {
        "total_coins": total_coins(conn),
        "total_market_cap": total_market_cap(conn),
        "avg_price": avg_price(conn),
        "top_coin": top_coin(conn),
        "top_gainer": top_gainer(conn),
        "top_loser": top_loser(conn),
    }

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Coins", kpis["total_coins"])
    st.metric("Avg Price", round(kpis["avg_price"] or 0, 6))
    st.metric("Market Cap", kpis["total_market_cap"])

with col2:
    st.subheader("🏆 Top Coin")

    if kpis["top_coin"]:
        st.success(
            f"{kpis['top_coin']['name']}\n"
            f"Market Cap: {kpis['top_coin']['market_cap']}"
        )

with col3:
    st.subheader("")
    st.write("")
    if kpis["top_gainer"]:
        st.success(
            f"🚀 {kpis['top_gainer']['name']} "
            f"(+{kpis['top_gainer']['change']}%)"
        )

    if kpis["top_loser"]:
        st.error(
            f"📉 {kpis['top_loser']['name']} "
            f"({kpis['top_loser']['change']}%)"
        )

# -----------------------------
# ✔ FIX: CACHED MARKET DATA
# -----------------------------
@st.cache_data(ttl=60)
def load_market_data():
    with engine.connect() as conn:
        return get_market_data(conn)

market_data = load_market_data()

# -----------------------------
# SAFETY CHECK
# -----------------------------
if not market_data:
    st.warning("No market data available")
    st.stop()

# -----------------------------
# CHART SECTION
# -----------------------------
st.divider()
st.subheader("📊 Market Insights")

st.caption("Market capitalization distribution across top crypto assets")

st.plotly_chart(
    bar_market_cap_chart(market_data),
    use_container_width=True
)

st.caption("Ranking of cryptocurrencies by market cap")

st.plotly_chart(
    line_price_change_chart(market_data),
    use_container_width=True
)

st.caption("24-hour price movement trends (gainers vs losers impact)")

st.plotly_chart(
    pie_market_share_chart(market_data),
    use_container_width=True
)

st.caption("Market share distribution of top crypto assets")

# -----------------------------
# DETAILS SECTION
# -----------------------------
@st.cache_data(ttl=60)
def load_details():
    with engine.connect() as conn:
        return get_crypto_details(conn)

details = load_details()

st.divider()
st.subheader("📋 Crypto Market Details TOP 5 Coins performance")

df = pd.DataFrame(
    details,
    columns=[
        "name",
        "symbol",
        "price",
        "market_cap",
        "rank",
        "24h_change",
        "volume"
    ]
)

df = df.sort_values("market_cap", ascending=False)

df["price"] = df["price"].round(4)
df["market_cap"] = df["market_cap"].round(2)
df["24h_change"] = df["24h_change"].round(2)

for _, row in df.iterrows():
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.write(f"**{row['name']} ({row['symbol']})**")

    with col2:
        st.write(f"💰 {row['price']}")

    with col3:
        st.write(f"📊 {row['market_cap']}")

    with col4:
        change = float(row["24h_change"])

        if change > 0:
            st.success(f"📈 {change}%")
        else:
            st.error(f"📉 {change}%")