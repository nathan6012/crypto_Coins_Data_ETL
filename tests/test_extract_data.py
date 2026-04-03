import pytest
from src.extract_data import extract_api_data
import respx
import httpx



@pytest.mark.asyncio
@respx.mock
async def test_extract_api_data_success(mocker):
    # Mock the API key
  mocker.patch("os.getenv", return_value="fake_api_key")
    
    # Mock the HTTP request
  respx.get("https://api.coingecko.com/api/v3/coins/markets").mock(
     return_value=httpx.Response(200, json=[{"id": "bitcoin"}])
    )

  result = await extract_api_data()
  assert result[0]["id"] == "bitcoin"