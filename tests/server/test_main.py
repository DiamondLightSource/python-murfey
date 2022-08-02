from __future__ import annotations

from fastapi.testclient import TestClient

from murfey.server.main import app

client = TestClient(app)


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert "<title>" in response.text.lower()


def test_get_visits():
    response = client.get("/visits")
    assert response.status_code == 200
    # assert response.json()[0]["Start date"] == "2020-09-09T14:00:00"


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
