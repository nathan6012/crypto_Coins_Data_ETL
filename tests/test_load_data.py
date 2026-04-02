
import pytest
import os
from sqlalchemy import create_engine, text
from src.load_data import load_data_to_db

@pytest.fixture
def temp_db(tmp_path, monkeypatch):
  """Creates a temporary file-based DB for the test."""
    # Create a real file path in a temp directory
  db_file = tmp_path / "test_crypto.db"
  temp_url = f"sqlite:///{db_file}"
    
    # Patch the db_url in your source file
  monkeypatch.setattr("src.load_data.db_url", temp_url)
  
  return temp_url

def test_load_data_upsert(temp_db):
    # 1. First record (Insert)
  records = [{"symbol": "btc", "name": "Bitcoin", "current_price": 50000.0}]
  load_data_to_db(records)
    
    # 2. Update same symbol (Upsert)
  updated = [{"symbol": "btc", "name": "Bitcoin", "current_price": 60000.0}]
  load_data_to_db(updated)

    # 3. Verify using the same temp_db URL
  engine = create_engine(temp_db)
  with engine.connect() as conn:
    result = conn.execute(text("SELECT current_price FROM crypto_markets WHERE symbol='btc'")).fetchone()
    
    # Check that the price was updated to 60000
  assert result[0] == 60000.0
