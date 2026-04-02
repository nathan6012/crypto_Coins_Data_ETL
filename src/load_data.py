from datetime import datetime
from sqlalchemy import create_engine 
from sqlalchemy import text,select,update 
from sqlalchemy import inspect 
from sqlalchemy import insert,Text,Float
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import UniqueConstraint 
from sqlalchemy import(Table,Column,Integer,String,MetaData,ForeignKey,Index)
from pathlib import Path
from sqlalchemy import DateTime


dir_url = Path(__file__).resolve().parent
root_dir = dir_url.parent
storage = root_dir/"data"

file = storage/"CryptoDB.db"
db_url = f"sqlite:///{file}"


def load_data_to_db(records): # change to add data 
  #Engine
  engine = create_engine(db_url)#echo=True)
  #MetaData 
  meta_obj = MetaData()
  #inspector 
  inspector = inspect(engine)
  tables = inspector.get_table_names()
  
  #insert to update on conflict 
  crypto_markets = Table(
    "crypto_markets",
    meta_obj,

    Column("id", Integer, primary_key=True, autoincrement=True),
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
  meta_obj.create_all(engine)
  
  
  
  
#Our insert /Update everytime   
  
  with engine.connect() as conn:
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
    conn.execute(stmt)
    conn.commit()  
  




  
  
      
      
      
  with engine.connect() as conn:
    stmt = (select(crypto_markets.c.ath))
    query = conn.execute(stmt)
    for row in query:
      print(row)
    
    
    
    

#All this is tes
def main():
  load_data_to_db()

if __name__ == "__main__":
  main()
  
  
  
  
  