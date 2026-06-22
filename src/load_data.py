import sys
import os
import asyncio

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import (
    select,
    Text, Float, DateTime,
    Table, Column, Integer, String,
    MetaData, Index
)

from sqlalchemy.dialects.postgresql import insert as pg_insert
from dotenv import load_dotenv
from shared.system_manager import setup_logger

# ======================
# LOGGING
# ======================
logging, log_file = setup_logger()

load_dotenv()

# ======================
# DATABASE
# ======================
db_url = os.getenv("DATABASE_URL").strip()
engine = create_async_engine(db_url, echo=False)

meta_obj = MetaData()

# ======================
# TABLES
# ======================
crypto_markets = Table(
    "crypto_markets",
    meta_obj,

    Column("id", String, primary_key=True),
    Column("symbol", String(50), unique=True, nullable=False),
    Column("name", String(255), nullable=False),

    Column("current_price", Float),
    Column("market_cap", Float),
    Column("market_cap_rank", Integer),
    Column("fully_diluted_valuation", Float),
    Column("total_volume", Float),

    Column("high_24h", Float),
    Column("low_24h", Float),
    Column("price_change_24h", Float),
    Column("price_change_percentage_24h", Float),

    Column("market_cap_change_24h", Float),
    Column("market_cap_change_percentage_24h", Float),

    Column("circulating_supply", Float),
    Column("total_supply", Float),
    Column("max_supply", Float),

    Column("ath", Float),
    Column("ath_change_percentage", Float),
    Column("ath_date", DateTime),
    Column("atl", Float),
    Column("atl_change_percentage", Float),
    Column("atl_date", DateTime),

    Column("roi", Text),
    Column("roi_times", Float),
    Column("roi_currency", String(50)),
    Column("roi_percentage", Float),

    Column("last_updated", DateTime),
)

Index("ix_crypto_markets_symbol", crypto_markets.c.symbol)

# ======================
# MAIN ETL FLOW (FULL LOAD)
# ======================
async def run_full_load(records):
    async with engine.begin() as conn:

        # Create table if not exists
        await conn.run_sync(meta_obj.create_all)

        # ======================
        # UPSERT ALL DATA
        # ======================
        if records:

            stmt = pg_insert(crypto_markets).values(records)

            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "symbol": stmt.excluded.symbol,
                    "name": stmt.excluded.name,
                    "current_price": stmt.excluded.current_price,
                    "market_cap": stmt.excluded.market_cap,
                    "market_cap_rank": stmt.excluded.market_cap_rank,
                    "fully_diluted_valuation": stmt.excluded.fully_diluted_valuation,
                    "total_volume": stmt.excluded.total_volume,

                    "high_24h": stmt.excluded.high_24h,
                    "low_24h": stmt.excluded.low_24h,
                    "price_change_24h": stmt.excluded.price_change_24h,
                    "price_change_percentage_24h": stmt.excluded.price_change_percentage_24h,

                    "market_cap_change_24h": stmt.excluded.market_cap_change_24h,
                    "market_cap_change_percentage_24h": stmt.excluded.market_cap_change_percentage_24h,

                    "circulating_supply": stmt.excluded.circulating_supply,
                    "total_supply": stmt.excluded.total_supply,
                    "max_supply": stmt.excluded.max_supply,

                    "ath": stmt.excluded.ath,
                    "ath_change_percentage": stmt.excluded.ath_change_percentage,
                    "ath_date": stmt.excluded.ath_date,
                    "atl": stmt.excluded.atl,
                    "atl_change_percentage": stmt.excluded.atl_change_percentage,
                    "atl_date": stmt.excluded.atl_date,

                    "roi": stmt.excluded.roi,
                    "roi_times": stmt.excluded.roi_times,
                    "roi_currency": stmt.excluded.roi_currency,
                    "roi_percentage": stmt.excluded.roi_percentage,

                    "last_updated": stmt.excluded.last_updated,
                }
            )

            await conn.execute(stmt)

        logging.info("Crypto markets FULL LOAD ETL completed successfully")