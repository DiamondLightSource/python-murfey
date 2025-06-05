from unittest.mock import ANY

from fastapi.testclient import TestClient
from sqlmodel import Session

from murfey.server.api.auth import validate_instrument_token
from murfey.server.main import app
from murfey.server.murfey_db import murfey_db
from murfey.util.api import url_path_for
from murfey.util.db import Movie

# @pytest.fixture(scope="module")
# def test_user():
#     return {"username": "testuser", "password": "testpass"}


# def movies_return():
#     return [("Supervisor_1", 2)]


# expression = Mock()
# expression.all = movies_return

# mock_session = create_autospec(Session, instance=True)
# mock_session.exec.return_value = expression


# def override_murfey_db():
#     try:
#         db = mock_session
#         yield db
#     finally:
#         db.close()


# app.dependency_overrides[murfey_db] = override_murfey_db

client = TestClient(app)


def test_movie_count(
    murfey_db_session: Session,
):

    # Insert test movies into Murfey DB
    tag = "test_movie"
    num_movies = 5
    murfey_db_session
    for i in range(num_movies):
        movie_db_entry = Movie(
            murfey_id=i,
            path="/some/path",
            image_number=i,
            tag=tag,
        )
        murfey_db_session.add(movie_db_entry)
        murfey_db_session.commit()

    # Replace the murfey_db instance in endpoint with properly initialised pytest one
    app.dependency_overrides[murfey_db] = murfey_db_session
    # Disable instrument token validation
    app.dependency_overrides[validate_instrument_token] = lambda: None

    response = client.get(
        f"{url_path_for('session_control.router', 'count_number_of_movies')}",
        headers={"Authorization": f"Bearer {ANY}"},
    )
    assert response.json() == {tag: num_movies}
