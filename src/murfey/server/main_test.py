from __future__ import annotations

from fastapi.testclient import TestClient

from murfey.server.main import app

client = TestClient(app)


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Transfer Server"}


def test_get_visits():
    response = client.get("/visits/m12")
    assert response.status_code == 200
    # assert response.json()[0]["Start date"] == "2020-09-09T14:00:00"


def test_client_hostname():
    response = client.get("/")
    assert response.status_code == 200


def test_pypi_proxy():
    response = client.get("/pypi/fastapi")
    assert response.status_code == 200


def test_get_microscope():
    response = client.get("/microscope")
    assert response.status_code == 200
    print(response.content)


def test_no_response():
    response = client.get("/hstnnsv")
    assert response.status_code != 200
