import os
import sys

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
)

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.load_data import run_full_load


# =========================
# FIXTURE
# =========================

@pytest.fixture
def sample_records():

    return [
        {
            "id": "btc",
            "symbol": "BTC",
            "name": "Bitcoin",
            "current_price": 50000,
            "market_cap": 1000000000,
            "last_updated": None,
        },
        {
            "id": "eth",
            "symbol": "ETH",
            "name": "Ethereum",
            "current_price": 3000,
            "market_cap": 500000000,
            "last_updated": None,
        },
    ]


# =========================
# SUCCESS LOAD TEST
# =========================

@pytest.mark.asyncio
@patch("src.load_data.engine")
async def test_full_load_success(
    mock_engine,
    sample_records
):
    """
    Should create tables and insert records
    """

    mock_conn = AsyncMock()

    mock_context = AsyncMock()

    mock_context.__aenter__.return_value = mock_conn


    mock_engine.begin.return_value = mock_context


    await run_full_load(sample_records)


    # create_all called
    mock_conn.run_sync.assert_called_once()


    # insert executed
    mock_conn.execute.assert_called_once()



# =========================
# EMPTY DATA TEST
# =========================

@pytest.mark.asyncio
@patch("src.load_data.engine")
async def test_full_load_empty(
    mock_engine
):
    """
    Should not insert when records are empty
    """

    mock_conn = AsyncMock()

    mock_context = AsyncMock()

    mock_context.__aenter__.return_value = mock_conn


    mock_engine.begin.return_value = mock_context


    await run_full_load([])


    mock_conn.execute.assert_not_called()



# =========================
# DATABASE FAILURE TEST
# =========================

@pytest.mark.asyncio
@patch("src.load_data.engine")
async def test_full_load_database_failure(
    mock_engine,
    sample_records
):
    """
    Should raise error when database fails
    """

    mock_conn = AsyncMock()

    mock_conn.execute.side_effect = Exception(
        "Database error"
    )


    mock_context = AsyncMock()

    mock_context.__aenter__.return_value = mock_conn


    mock_engine.begin.return_value = mock_context


    with pytest.raises(Exception):

        await run_full_load(sample_records)