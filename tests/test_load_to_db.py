import sys
import os 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import AsyncMock, MagicMock
from src.load_data import load_data_to_db  # replace with your file


@pytest.mark.asyncio
async def test_load_data_to_db(monkeypatch):
    
    # -------------------------
    # Mock ENV
  monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

    # -------------------------
    # Mock connection object
  mock_conn = AsyncMock()
  mock_conn.run_sync = AsyncMock()
  mock_conn.execute = AsyncMock()

    # context manager for engine.begin()
  class MockBegin:
    async def __aenter__(self):
      return mock_conn

    async def __aexit__(self, exc_type, exc, tb):
      pass

    # -------------------------
    # Mock engine
  mock_engine = MagicMock()
  mock_engine.begin.return_value = MockBegin()

    # Patch create_async_engine
  monkeypatch.setattr(
    "src.load_data.create_async_engine",
    lambda *args, **kwargs: mock_engine )

    # -------------------------
    # Fake records
  records = [
        {
            "id": "btc",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 50000
        }
    ]

    # -------------------------
    # Run function
  await load_data_to_db(records)

    # -------------------------
    # Assertions
  assert mock_conn.run_sync.called  # table creation
  assert mock_conn.execute.called   # insert executed
