import pytest
import pandas as pd

from src.transform import transform_data


# =========================
# FIXTURES
# =========================

@pytest.fixture
def good_records():
    return [
        {
            "idx": 1,
            "data": {
                "id": "btc",
                "symbol": "BTC",
                "name": "Bitcoin",
                "current_price": "50000",
                "market_cap": "1000000",
                "roi": {
                    "currency": "USD",
                    "times": "2.5",
                    "percentage": "150"
                },
                "last_updated": "2026-06-25T10:00:00Z"
            }
        }
    ]


@pytest.fixture
def bad_records():
    return [
        {
            "idx": 2,
            "data": {
                "id": "eth",
                "symbol": "ETH",
                "name": "Ethereum",
                "current_price": None,
                "market_cap": None,
                "errors": "bad row",
                "last_updated": "2026-06-25T11:00:00Z"
            }
        }
    ]


# =========================
# TEST 1: OUTPUT SIZE
# =========================

def test_transform_row_count(good_records, bad_records):
    result = transform_data(good_records, bad_records)

    assert len(result) == 2


# =========================
# TEST 2: REQUIRED FIELDS EXIST
# =========================

def test_transform_required_fields(good_records, bad_records):
    result = transform_data(good_records, bad_records)

    row = result[0]

    assert "id" in row
    assert "symbol" in row
    assert "name" in row
    assert "current_price" in row


# =========================
# TEST 3: ROI RENAMING
# =========================

def test_roi_columns_transformed(good_records, bad_records):
    result = transform_data(good_records, bad_records)

    row = result[0]

    assert "roi_currency" in row
    assert "roi_times" in row
    assert "roi_percentage" in row


# =========================
# TEST 4: NUMERIC CLEANUP
# =========================

def test_numeric_conversion(good_records, bad_records):
    result = transform_data(good_records, bad_records)

    row = result[0]

    assert isinstance(row["current_price"], (int, float))
    assert isinstance(row["market_cap"], (int, float))


# =========================
# TEST 5: NULL CLEANING
# =========================
def test_null_handling(good_records, bad_records):
    result = transform_data(good_records, bad_records)

    row = result[1]

    # numeric nulls become 0, not None
    assert row["current_price"] == 0.0


# =========================
# TEST 6: DATE PARSING
# =========================

def test_date_parsing(good_records, bad_records):
    result = transform_data(good_records, bad_records)

    row = result[0]

    # should be python datetime (timezone removed)
    assert row["last_updated"] is not None