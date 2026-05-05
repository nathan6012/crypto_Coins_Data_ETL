import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import asyncio
from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import (
    text, select, update,
    Text, Float, DateTime,
    Table, Column, Integer, String,
    MetaData, Index
)

from sqlalchemy.dialects.postgresql import insert as pg_insert
from dotenv import load_dotenv
from src.system_manager import setup_logger


logging, log_file = setup_logger()

load_dotenv()

# ======================
# DATABASE / META (GLOBAL SCOPE FIX)
# ======================

db_url = os.getenv("DATABASE_URL").strip()
engine = create_async_engine(db_url, echo=False)
meta_obj = MetaData()

etl_state = Table(
    "etl_state",
    meta_obj,
    Column("pipeline_name", String, primary_key=True),
    Column("last_seen", DateTime),
)

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
# HELPERS
# ======================

async def get_last_seen(conn):
    result = await conn.execute(
        select(etl_state.c.last_seen).where(
            etl_state.c.pipeline_name == "users_pipeline"
        )
    )
    row = result.fetchone()
    return row[0] if row else None


async def get_new_rows(conn, last_seen):
    query = select(crypto_markets)

    if last_seen:
        query = query.where(crypto_markets.c.last_updated > last_seen)

    result = await conn.execute(query)
    return [dict(r._mapping) for r in result.fetchall()]


async def update_last_seen(conn, rows):
    if not rows:
        return

    latest = max(r["last_updated"] for r in rows)

    stmt = pg_insert(etl_state).values(
        pipeline_name="users_pipeline",
        last_seen=latest
    ).on_conflict_do_update(
        index_elements=["pipeline_name"],
        set_={"last_seen": latest}
    )

    await conn.execute(stmt)








# ======================
# MAIN ETL FLOW
# ======================

async def run_incremental(records):
  async with engine.begin() as conn:
    await conn.run_sync(meta_obj.create_all)

    last_seen = await get_last_seen(conn)

        # 3. filter incoming data (optional but useful)
    if last_seen:
      records = [
        r for r in records
        if r.get("last_updated") and r["last_updated"] > last_seen
            ]

        # 4. upsert data
    if records:
      stmt = pg_insert(crypto_markets).values(records)

      stmt = stmt.on_conflict_do_update(
        index_elements=["symbol"],
        set_={col.name: getattr(stmt.excluded, col.name)
        for col in crypto_markets.c 
        if col.name != "id"})
      await conn.execute(stmt)

        # 5. update checkpoint
    await update_last_seen(conn, records)
    logging.info("Data Updated/Staged")


# ======================
# ENTRY POINT
# ======================

#asyncio.run(run_incremental(records))