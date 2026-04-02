import pytest
import respx
import httpx
import os 
#from prefect.testing.utilities import prefect_test_harness
from src.extract_data import extract_api_data 

# 1. Setup the Prefect Test Harness as a fixture
# This ensures every test runs in an isolated temporary environment
@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
  os.environ["PREFECT_URL"] = ""
  yield

# 2. Define the Test Case

@pytest.mark.asyncio
@respx.mock
async def test_extract_api_data_success():
  """Test that extract_api_data correctly fetches and returns mocked data"""
    
    # Arrange: Setup the mock response for the specific CoinGecko URL
  mock_url = "https://api.coingecko.com/api/v3/coins/markets"
  mock_data = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 50000},
        {"id": "ethereum", "symbol": "eth", "current_price": 3000}
    ]
  
  respx.get(mock_url, params={"vs_currency": "usd"}).respond(json=mock_data)


    # Act: Call your function
  result = await extract_api_data()

    # Assert: Verify the result matches our mock data
  assert isinstance(result, list)
  assert len(result) == 2
  assert result[0]["id"] == "bitcoin"
  assert len(respx.calls) == 1

 # Verifies the API was actually hit
