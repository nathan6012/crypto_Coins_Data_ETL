import os
import sys

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
)

from unittest.mock import patch, MagicMock
import pytest

from src.save_raw_data import save_raw_data


# =========================
# FIXTURES
# =========================

@pytest.fixture
def sample_data():
    return [
        {
            "symbol": "BTC",
            "price": 50000
        },
        {
            "symbol": "ETH",
            "price": 3000
        }
    ]


@pytest.fixture
def mock_s3():
    return MagicMock()


# =========================
# SUCCESS TEST
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_success(
    mock_boto_client,
    mock_s3,
    sample_data
):
    """
    Should upload JSON data to object storage
    """

    # Arrange
    mock_boto_client.return_value = mock_s3

    # Act
    save_raw_data(sample_data)

    # Assert client created
    mock_boto_client.assert_called_once()

    # Assert upload happened
    mock_s3.put_object.assert_called_once()

    _, kwargs = mock_s3.put_object.call_args

    assert kwargs["Bucket"] == "nathan-elt-buck"

    assert "crypto/json/" in kwargs["Key"]

    assert kwargs["ContentType"] == "application/json"

    assert isinstance(
        kwargs["Body"],
        bytes
    )


# =========================
# EMPTY DATA TEST
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_empty(
    mock_boto_client,
    mock_s3
):
    """
    Should not upload when data is empty
    """

    # Arrange
    mock_boto_client.return_value = mock_s3

    # Act
    save_raw_data([])

    # Assert
    mock_s3.put_object.assert_not_called()


# =========================
# UPLOAD FAILURE TEST
# =========================

@patch("src.save_raw_data.boto3.client")
def test_save_raw_data_upload_failure(
    mock_boto_client,
    mock_s3,
    sample_data
):
    """
    Should raise error when storage upload fails
    """

    # Arrange
    mock_s3.put_object.side_effect = Exception(
        "Storage upload failed"
    )

    mock_boto_client.return_value = mock_s3


    # Act + Assert
    with pytest.raises(Exception):
        save_raw_data(sample_data)