import copy
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.api.auth import submit_to_auth_endpoint


def test_check_user():
    pass


@pytest.mark.parametrize(
    "test_params",
    (  # URL subpath | Auth type | Status code | Validation result
        (
            "validate_token",
            "cookie",
            200,
            True,
        ),
        (
            "validate_visit_access/some_visit",
            "password",
            200,
            True,
        ),
        (
            "validate_instrument_access/some_instrument",
            "cookie",
            200,
            False,
        ),
        (
            "validate_token",
            "password",
            200,
            False,
        ),
        (
            "validate_visit_access/some_visit",
            "cookie",
            400,
            True,
        ),
        (
            "validate_instrument_access/some_instrument",
            "password",
            400,
            True,
        ),
    ),
)
@pytest.mark.asyncio
async def test_submit_to_auth_endpoint(
    mocker: MockerFixture,
    test_params: tuple[str, str, int, bool],
):
    # Unpack test params
    url_subpath, auth_type, status_code, validation_outcome = test_params

    # Patch the auth URL to use
    auth_url = "some_url"
    mocker.patch("murfey.server.api.auth.auth_url", auth_url)

    # Patch the security config
    mock_security_config = MagicMock()
    mock_security_config.auth_url = auth_url
    mock_security_config.auth_type = auth_type
    mock_security_config.cookie_key = "_oauth2_proxy"
    mocker.patch("murfey.server.api.auth.security_config", mock_security_config)

    # Mock the request being forwarded and its headers and cookies
    mock_headers = {
        "authorization": "Bearer dummy",
        "x-auth-request-access-token": "dummy",
    }
    mock_token = "123456"
    mock_cookies = (
        {mock_security_config.cookie_key: mock_token} if auth_type == "cookie" else {}
    )

    mock_request = MagicMock()
    mock_request.headers = mock_headers

    # Mock the async response
    mock_response = MagicMock()
    mock_response.status = status_code
    mock_response.json = AsyncMock(
        return_value={
            "valid": validation_outcome,
        }
    )

    # Mock the async session and the 'get'
    mock_get = AsyncMock()
    mock_get.__aenter__.return_value = mock_response

    mock_session = MagicMock()
    mock_session.get.return_value = mock_get

    mock_session_context = AsyncMock()
    mock_session_context.__aenter__.return_value = mock_session

    mock_client_session = mocker.patch(
        "murfey.server.api.auth.aiohttp.ClientSession",
        return_value=mock_session_context,
    )

    # Run the function and check that the correct calls were made
    result = await submit_to_auth_endpoint(
        url_subpath=url_subpath,
        request=mock_request,
        token=mock_token,
    )

    # Check that aiohttp.ClientSession got called with the correct parameters
    mock_client_session.assert_called_once_with(cookies=mock_cookies)

    # Compare the headers passed to 'session.get' against what is expected
    updated_headers = copy.deepcopy(mock_headers)
    if auth_type == "password":
        updated_headers["authorization"] = f"Bearer {mock_token}"
    mock_session.get.assert_called_once_with(
        f"{mock_security_config.auth_url}/{url_subpath}",
        headers=updated_headers,
    )

    # Check that the combination of status code and JSON response are correct
    assert result == {"valid": (validation_outcome if status_code == 200 else False)}


@pytest.mark.asyncio
async def test_validate_token():
    pass


def test_validate_session_against_visit():
    pass


@pytest.mark.asyncio
async def test_validate_instrument_token():
    pass


def test_get_visit_name():
    pass


@pytest.mark.asyncio
async def test_validate_instrument_server_session_access():
    pass


@pytest.mark.asyncio
async def test_validate_frontend_session_access():
    pass


@pytest.mark.asyncio
async def test_validate_user_instrument_access():
    pass


def test_verify_password():
    pass


def test_validate_user():
    pass


def test_create_access_token():
    pass


@pytest.mark.asyncio
async def test_generate_token():
    pass


@pytest.mark.asyncio
async def test_mint_session_token():
    pass
