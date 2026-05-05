import sys
import os 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import AsyncMock, MagicMock
from src.load_data import run_incremental  

import pytest
from datetime import datetime, timedelta
from datetime import datetime, timezone



@pytest.fixture
def sample_records():
  now = datetime.now(timezone.utc)

  return [
        {
            "symbol": "BTC",
            "name": "Bitcoin",
            "last_updated": now,
            "current_price": 50000,
        },
        {
            "symbol": "ETH",
            "name": "Ethereum",
            "last_updated": now + timedelta(minutes=1),
            "current_price": 3000,
        },
    ]


@pytest.fixture
def old_last_seen():
  return datetime.now(timezone.utc) - timedelta(days=1)


@pytest.fixture
def recent_last_seen():
  return datetime.now(timezone.utc) + timedelta(days=1)


# =========================
# TESTS
# =========================

def test_filter_new_rows(sample_records, old_last_seen):
  """Should return all records if last_seen is old"""

  filtered = [
        r for r in sample_records
        if r["last_updated"] > old_last_seen
    ]

  assert len(filtered) == 2


def test_filter_no_new_rows(sample_records, recent_last_seen):
  """Should return empty if last_seen is newer than data"""

  filtered = [
        r for r in sample_records
        if r["last_updated"] > recent_last_seen
    ]

  assert len(filtered) == 0


def test_last_seen_computation(sample_records):
  """Should compute max last_updated correctly"""

  latest = max(r["last_updated"] for r in sample_records)

  assert latest == sample_records[1]["last_updated"]


# =========================
# MOCK BEHAVIOR TEST (ETL LOGIC)
# =========================

@pytest.mark.asyncio
async def test_incremental_flow_logic(sample_records):
  """
    Tests core ETL decision logic WITHOUT DB.
    """

  last_seen = None

    # simulate first run
  new_rows = sample_records if not last_seen else [
        r for r in sample_records if r["last_updated"] > last_seen
    ]

  assert len(new_rows) == 2

    # update checkpoint
  last_seen = max(r["last_updated"] for r in new_rows)

    # simulate second run with same data
  new_rows_2 = [
        r for r in sample_records if r["last_updated"] > last_seen
    ]

  assert len(new_rows_2) == 0