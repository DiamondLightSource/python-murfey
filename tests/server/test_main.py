from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from murfey.server.main import app

client = TestClient(app)


@pytest.fixture(scope="module")
def test_user():
    return {"username": "testuser", "password": "testpass"}


def login(test_user):
    with patch(
        "murfey.server.auth.api.validate_user", return_value=True
    ) as mock_validate:
        response = client.post("/token", data=test_user)
        assert mock_validate.called_once()
        assert response.status_code == 200
        token = response.json()["access_token"]
        assert token is not None
        return token


@patch("murfey.server.auth.check_user", return_value=True)
def test_read_main(mock_check, test_user):
    token = login(test_user)
    response = client.get("/", headers={"Authorization": f"Bearer {token}"})
    assert mock_check.called_once()
    assert response.status_code == 200
    assert "<title>" in response.text.lower()


def test_pypi_proxy():
    response = client.get("/pypi/fastapi")
    assert response.status_code == 200
    assert "<a href" in response.text.lower()
    assert ".tar.gz" in response.text.lower()


def test_file_not_found_response():
    response = client.get("/hstnnsv")
    assert response.status_code == 404


def test_openapi_json_is_valid():
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json()
