import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from src.save_raw_data import save_raw_data

@pytest.fixture
def sample_data():
  return [
        {"symbol": "BTC", "price": 50000},
        {"symbol": "ETH", "price": 3000},
    ]


# =========================
# TEST 1: SUCCESS PATH
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_success(mock_boto_client, sample_data):
  """Should upload data to S3/R2 with correct payload"""

  mock_s3 = MagicMock()
  mock_boto_client.return_value = mock_s3

  save_raw_data(sample_data)

    # must create client
  mock_boto_client.assert_called_once()

    # must call put_object once
  assert mock_s3.put_object.called

  args, kwargs = mock_s3.put_object.call_args

  assert kwargs["Bucket"] == "nathan-elt-buck"
  assert "crypto/json/" in kwargs["Key"]
  assert kwargs["ContentType"] == "application/json"

    # Body should be bytes
  assert isinstance(kwargs["Body"], bytes)


# =========================
# TEST 2: EMPTY DATA
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_empty(mock_boto_client):
  """
    Should NOT call S3 if data is empty
    """

  mock_s3 = MagicMock()
  mock_boto_client.return_value = mock_s3

  save_raw_data([])

  mock_s3.put_object.assert_not_called()