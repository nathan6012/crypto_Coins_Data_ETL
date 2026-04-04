from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine
import asyncio

from sqlalchemy import text,select,update 
#from sqlalchemy import inspect 
from sqlalchemy import Text,Float
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import UniqueConstraint 
from sqlalchemy import(Table,Column,Integer,String,MetaData,ForeignKey,Index)
from sqlalchemy import DateTime

from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

load_dotenv()



async def load_data_to_db(records):  
  
  
  db_url = os.getenv("DATABASE_URL").strip()
  #Engine
  engine = create_async_engine(db_url,echo=False)#echo=True)
  #MetaData 
  meta_obj = MetaData()

  
#  inspector = inspect(engine)
  #tables = inspector.get_table_names()
  
  #insert to update on conflict 
  crypto_markets = Table(
    "crypto_markets",
    meta_obj,

    Column("id", String, primary_key=True,),
    Column("symbol", String(50), unique=True, nullable=False),
    Column("name", String(255), nullable=False),

    Column("current_price", Float, nullable=True),
    Column("market_cap", Float, nullable=True),
    Column("market_cap_rank", Integer, nullable=True),
    Column("fully_diluted_valuation", Float, nullable=True),
    Column("total_volume", Float, nullable=True),

    Column("high_24h", Float, nullable=True),
    Column("low_24h", Float, nullable=True),
    Column("price_change_24h", Float, nullable=True),
    Column("price_change_percentage_24h", Float, nullable=True),

    Column("market_cap_change_24h", Float, nullable=True),
    Column("market_cap_change_percentage_24h", Float, nullable=True),
    Column("circulating_supply", Float, nullable=True),
    Column("total_supply", Float, nullable=True),
    Column("max_supply", Float, nullable=True),

    Column("ath", Float, nullable=True),
    Column("ath_change_percentage", Float, nullable=True),
    Column("ath_date", DateTime, nullable=True),
    Column("atl", Float, nullable=True),
    Column("atl_change_percentage", Float, nullable=True),
    Column("atl_date", DateTime, nullable=True),

    Column("roi", Text, nullable=True),
    Column("last_updated", DateTime, nullable=True),
    Column("roi_times", Float, nullable=True),
    Column("roi_currency", String(50), nullable=True),
    Column("roi_percentage", Float, nullable=True),
)
  
  #Faster Query   
  Index("ix_crypto_markets_symbol", crypto_markets.c.symbol)
  
  
  
  #Create The tables # Meta_obj must be closed
  # to avoid Db creation Erorrs 
 # ____________________________
  async with engine.begin() as conn:
    await conn.run_sync(meta_obj.create_all)
    
  
  
  
  
#Our insert /Update everytime   
  
  async with engine.begin() as conn:
    stmt = insert(crypto_markets).values(records)
    stmt = stmt.on_conflict_do_update(
      index_elements=["symbol"],
      set_={
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
        "last_updated": stmt.excluded.last_updated,
        "roi_times": stmt.excluded.roi_times,
        "roi_currency": stmt.excluded.roi_currency,
        "roi_percentage": stmt.excluded.roi_percentage,

        } )
    await conn.execute(stmt)
 
      
  #with engine.connect() as conn:
  #  stmt = (select(crypto_markets.c.name))
   # query = conn.execute(stmt)
  #  for row in query:
    #  print(row)
      
 # with engine.connect() as conn:
  #  stmt = (select(crypto_markets.c.id))
   # query = conn.execute(stmt)
  #  for row in query:
   #   print(row)  
    
    
    

#All this is tes
async def main():
  await load_data_to_db()

if __name__ == "__main__":
  asyncio.run(main())
  
  
  
  
  