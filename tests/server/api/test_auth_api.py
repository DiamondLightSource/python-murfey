import copy
import secrets
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from pytest_mock import MockerFixture

from murfey.server.api.auth import (
    submit_to_auth_endpoint,
    validate_token,
)


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


@pytest.mark.parametrize(
    "test_params",
    (  # Exception raised? | Auth URL | Auth type | Validation outcome | User decoded | User exists
        (False, "some_url", "cookie", True, True, True),
        (False, "", "password", True, True, True),
        # Auth endpoint returns False
        (True, "some_url", "cookie", False, True, True),
        # Authenticating with cookie, but no auth URL
        (True, "", "cookie", True, True, True),
        # Decoding fails
        (True, "", "password", True, False, True),
        # User check fails
        (True, "", "password", True, True, False),
    ),
)
@pytest.mark.asyncio
async def test_validate_token(
    mocker: MockerFixture,
    test_params: tuple[bool, str, str, bool, bool, bool],
):
    # Unpack test params
    (
        raises_exception,
        auth_url,
        auth_type,
        validation_outcome,
        user_decoded,
        user_exists,
    ) = test_params

    # Patch the auth URL to use
    mocker.patch("murfey.server.api.auth.auth_url", auth_url)

    # Create a mock token
    mock_token = "some_token"

    # Mock the request
    mock_request = MagicMock()

    # Mock the secret key and algorithms module-level variables
    mock_secret_key = mocker.patch(
        "murfey.server.api.auth.SECRET_KEY", secrets.token_hex(32)
    )
    mock_algorithms = mocker.patch("murfey.server.api.auth.ALGORITHM", "HS256")

    # Mock the 'jwt.decode' function
    mock_decoded_data = {"user": "some_user"} if user_decoded else {}
    mock_decode = mocker.patch(
        "murfey.server.api.auth.jwt.decode", return_value=mock_decoded_data
    )

    # Mock the 'check_user' function
    mock_check_user = mocker.patch(
        "murfey.server.api.auth.check_user", return_value=user_exists
    )

    # Patch the security config
    mock_security_config = MagicMock()
    mock_security_config.auth_type = auth_type
    mocker.patch("murfey.server.api.auth.security_config", mock_security_config)

    # Patch the 'submit_to_auth_endpoint' function
    mock_submit = mocker.patch(
        "murfey.server.api.auth.submit_to_auth_endpoint", new_callable=AsyncMock
    )
    mock_submit.return_value = {"valid": validation_outcome}

    # Run the function and check that the values passed and returned are as expected
    if not raises_exception:
        result = await validate_token(
            token=mock_token,
            request=mock_request,
        )
        if auth_url:
            mock_submit.assert_called_once_with(
                "validate_token", mock_request, mock_token
            )
        if auth_type == "password":
            mock_decode.assert_called_once_with(
                mock_token, mock_secret_key, algorithms=[mock_algorithms]
            )
            mock_check_user.assert_called_once_with(mock_decoded_data["user"])
        assert result is None
    else:
        with pytest.raises(HTTPException):
            await validate_token(
                token=mock_token,
                request=mock_request,
            )


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
