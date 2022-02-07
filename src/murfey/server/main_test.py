from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def client():
    pytest.xfail("Will fail if server not running")
    from murfey.server.main import app

    return TestClient(app)


def test_read_main(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Transfer Server"}


def test_get_visits(client):
    response = client.get("/visits/m12")
    assert response.status_code == 200
    # assert response.json()[0]["Start date"] == "2020-09-09T14:00:00"


def test_client_hostname(client):
    response = client.get("/")
    assert response.status_code == 200


def test_pypi_proxy(client):
    response = client.get("/pypi/fastapi")
    assert response.status_code == 200


def test_get_microscope(client):
    response = client.get("/microscope")
    assert response.status_code == 200
    print(response.content)


def test_no_response(client):
    response = client.get("/hstnnsv")
    assert response.status_code != 200
