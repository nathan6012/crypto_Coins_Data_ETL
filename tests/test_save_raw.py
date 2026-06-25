import os
import sys
from unittest.mock import patch, MagicMock
import pytest

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
)

from src.save_raw_data import save_raw_data


# =========================
# FIXTURES
# =========================

@pytest.fixture
def sample_data():
    return [
        {"symbol": "BTC", "price": 50000},
        {"symbol": "ETH", "price": 3000}
    ]


@pytest.fixture
def mock_s3():
    return MagicMock()


# =========================
# SUCCESS TEST
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_success(mock_boto_client, mock_s3, sample_data):

    mock_boto_client.return_value = mock_s3

    result = save_raw_data(sample_data)

    # client created
    mock_boto_client.assert_called_once()

    # upload called
    mock_s3.put_object.assert_called_once()

    _, kwargs = mock_s3.put_object.call_args

    assert kwargs["Bucket"] == "nathan-elt-buck"
    assert "crypto/json/" in kwargs["Key"]
    assert kwargs["ContentType"] == "application/json"
    assert isinstance(kwargs["Body"], bytes)

    # NEW: check return value
    assert result["status"] == "success"
    assert result["saved"] is True


# =========================
# EMPTY DATA TEST
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_empty(mock_boto_client, mock_s3):

    mock_boto_client.return_value = mock_s3

    result = save_raw_data([])

    mock_s3.put_object.assert_not_called()

    assert result["status"] == "empty"
    assert result["saved"] is False


# =========================
# FAILURE TEST
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_upload_failure(mock_boto_client, mock_s3, sample_data):

    mock_s3.put_object.side_effect = Exception("Storage upload failed")
    mock_boto_client.return_value = mock_s3

    result = save_raw_data(sample_data)

    # must NOT raise anymore
    assert result["status"] == "failed"
    assert result["saved"] is False
    assert "error" in result