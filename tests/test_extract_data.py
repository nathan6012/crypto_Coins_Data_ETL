import pytest
from src.extract_data import extract_api_data
import respx
import httpx


@pytest.mark.asyncio
@respx.mock
async def test_extract_api_data_success():
    # Mock the endpoint (ignore query string)
  respx.get("https://api.coingecko.com/api/v3/coins/markets").mock(
        return_value=httpx.Response(200, json=[{"id": "bitcoin"}])
    )
    
    # Call your async function
  data = await extract_api_data()

    # Assertions
  assert isinstance(data, list)
  assert data[0]["id"] == "bitcoin"

