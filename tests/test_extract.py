import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
import httpx
from src.extract_data import extract_api_data  #



class MockResponse:
  def __init__(self, json_data, status_code=200):
    self._json = json_data
    self.status_code = status_code

  def json(self):
    return self._json

  def raise_for_status(self):
    if self.status_code != 200:
      raise httpx.HTTPStatusError("Error", request=None, response=None)


@pytest.mark.asyncio
async def test_extract_api_data(monkeypatch):
    # Mock data returned by API
  mock_data = [{"id": "bitcoin"}, {"id": "ethereum"}]

  async def mock_get(*args, **kwargs):
    return MockResponse(mock_data)

    # Patch httpx AsyncClient.get
  monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

  result = await extract_api_data()

  assert isinstance(result, list)
  assert len(result) == 2
  assert result[0]["id"] == "bitcoin"

