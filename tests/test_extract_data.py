import pytest
from src.extract_data import extract_api_data
import respx
import httpx
from unittest.mock import AsyncMock, patch



@pytest.mark.asyncio
@respx.mock
async def test_extract_api_data_success():
    # Match the URL exactly as your code builds it
  respx.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd").mock(
        return_value=httpx.Response(200, json=[{"id": "bitcoin"}])
    )

  result = await extract_api_data()
    
  assert result == [{"id": "bitcoin"}]

@pytest.mark.asyncio
@respx.mock
async def test_extract_api_data_error():
    # Use a regex or prefix to match the URL regardless of params for the error test
  respx.get(url__startswith="https://api.coingecko.com/api/v3/coins/markets").mock(
        return_value=httpx.Response(500)
    )

  with pytest.raises(httpx.HTTPStatusError):
    await extract_api_data()
