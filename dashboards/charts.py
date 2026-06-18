import pandas as pd
import plotly.express as px



def bar_market_cap_chart(data):

    df = pd.DataFrame(data).copy()

    df = df[["name", "market_cap"]].dropna()

    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

    df = df.sort_values("market_cap", ascending=False).head(10)

    fig = px.bar(
        df,
        x="name",
        y="market_cap",
        title="Top 10 Cryptos by Market Cap"
    )

    return fig
    
    
    
def line_price_change_chart(data):

    df = pd.DataFrame(data).copy()

    df = df[["name", "price_change_percentage_24h"]].dropna()

    df["price_change_percentage_24h"] = pd.to_numeric(
        df["price_change_percentage_24h"],
        errors="coerce"
    )

    df = df.sort_values(
        "price_change_percentage_24h",
        ascending=False
    ).head(10)

    fig = px.line(
        df,
        x="name",
        y="price_change_percentage_24h",
        title="Top 10 24h Price Changes"
    )

    return fig    
    
    
def pie_market_share_chart(data):

    df = pd.DataFrame(data).copy()

    df = df[["name", "market_cap"]].dropna()

    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

    df = df.sort_values("market_cap", ascending=False).head(6)

    fig = px.pie(
        df,
        names="name",
        values="market_cap",
        title="Market Cap Distribution (Top 6 Coins)"
    )

    return fig    