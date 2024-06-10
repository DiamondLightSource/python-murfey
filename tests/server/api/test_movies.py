from unittest.mock import Mock, create_autospec, patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from murfey.server.main import app
from murfey.server.murfey_db import murfey_db_session


@pytest.fixture(scope="module")
def test_user():
    return {"username": "testuser", "password": "testpass"}


def movies_return():
    return [("Supervisor_1", 2)]


expression = Mock()
expression.all = movies_return

mock_session = create_autospec(Session, instance=True)
mock_session.exec.return_value = expression


def override_murfey_db():
    try:
        db = mock_session
        yield db
    finally:
        db.close()


app.dependency_overrides[murfey_db_session] = override_murfey_db

client = TestClient(app)


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
def test_movie_count(mock_check, test_user):
    token = login(test_user)
    response = client.get("/num_movies", headers={"Authorization": f"Bearer {token}"})
    assert mock_check.called_once()
    assert response.status_code == 200
    assert len(mock_session.method_calls) == 2
    assert response.json() == {"Supervisor_1": 2}
