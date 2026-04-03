import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.load_data import load_data_to_db  # Adjust this import based on your file name

@pytest.mark.asyncio
async def test_load_data_to_db_success(mocker):
    # 1. Setup Mock Records
  mock_records = [
        {"symbol": "btc", "name": "Bitcoin", "current_price": 50000.0},
        {"symbol": "eth", "name": "Ethereum", "current_price": 3000.0}
    ]

    # 2. Mock Environment Variable
  mocker.patch("os.getenv", return_value="postgresql+asyncpg://user:pass@localhost/db")

    # 3. Mock the Async Engine and Connection
    # We need to mock create_async_engine to return a mock engine
  mock_engine = MagicMock()
  mock_conn = AsyncMock()
    
    # engine.begin() is an async context manager
  mock_engine.begin.return_value.__aenter__.return_value = mock_conn
    
    # Patch the engine creation inside your function
  mock_create = mocker.patch("src.main.create_async_engine", return_value=mock_engine)

    # 4. Run the function
  await load_data_to_db(mock_records)

    # 5. Assertions
    # Verify engine was created
  mock_create.assert_called_once()
    
    # Verify the table creation was called
    # run_sync is used for meta_obj.create_all
  assert mock_conn.run_sync.called

    # Verify the execute was called for the insert/upsert
  assert mock_conn.execute.called
    
    # Check if the first argument of the execute call was our statement
  args, _ = mock_conn.execute.call_args
  assert "INSERT INTO crypto_markets" in str(args[0])
  assert "ON CONFLICT (symbol) DO UPDATE" in str(args[0])
