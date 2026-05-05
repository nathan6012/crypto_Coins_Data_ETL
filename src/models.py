import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pydantic import BaseModel, AnyUrl, ConfigDict, field_validator
from typing import Optional
from decimal import Decimal
from datetime import datetime

# Using Strict Validation only Needed fields 




class ROI(BaseModel):
  times: Optional[Decimal] = None
  currency: Optional[str] = None
  percentage: Optional[Decimal] = None
  
  @field_validator("*", mode="before")
  def clean_roi_fields(cls, v, info):
    field_name = info.field_name  # <- use this in v2
    if v in ("", None):
      return None
    if field_name in ["times", "percentage"]:
      return Decimal(str(v))
    return v

  
class CryptoCoins(BaseModel):
  # No extra unknow contract fields 
  model_config=ConfigDict(extra="forbid")
  
  id: str
  symbol: str
  name: str 
  image: Optional[AnyUrl] = None 
  current_price: Decimal
  market_cap: Optional[int] = None
  market_cap_rank:int 
  fully_diluted_valuation: Optional[int] = None
  total_volume: Optional[int]= None
  high_24h: Optional[Decimal] = None 
  low_24h: Optional[Decimal] = None
  price_change_24h: Optional[Decimal] = None
  price_change_percentage_24h:Optional[Decimal] = None
  market_cap_change_24h: int
  market_cap_change_percentage_24h: Decimal
  circulating_supply: Decimal
  total_supply: Decimal
  max_supply: Optional[Decimal] = None 
  
  ath: Optional[Decimal] = None 
  ath_change_percentage: Optional[Decimal] = None
  ath_date: datetime
  atl: Decimal
  atl_change_percentage: Decimal
  atl_date: datetime
  roi: Optional[ROI] = None
  last_updated: datetime 
  

  @field_validator("last_updated","ath_date","atl_date", mode="before")
  def clean_datetimes(cls, v):
        if v in ("", None):
            return None
        return v    
  
