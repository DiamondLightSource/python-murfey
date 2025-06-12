from __future__ import annotations

import secrets
import time
from logging import getLogger
from typing import Dict
from uuid import uuid4

import aiohttp
import requests
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import (
    APIKeyCookie,
    OAuth2PasswordBearer,
    OAuth2PasswordRequestForm,
)
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlmodel import Session, create_engine, select
from typing_extensions import Annotated

from murfey.server.murfey_db import murfey_db, url
from murfey.util.api import url_path_for
from murfey.util.config import get_security_config
from murfey.util.db import MurfeyUser as User
from murfey.util.db import Session as MurfeySession

# Set up logger
logger = getLogger("murfey.server.api.auth")

# Set up router
router = APIRouter(
    prefix="/auth",
    tags=["Authentication"],
)


# Set up variables used for authentication
security_config = get_security_config()
auth_url = security_config.auth_url
ALGORITHM = security_config.auth_algorithm or "HS256"
SECRET_KEY = security_config.auth_key or secrets.token_hex(32)
if security_config.auth_type == "password":
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
else:
    oauth2_scheme = APIKeyCookie(name=security_config.cookie_key)
if security_config.instrument_auth_type == "token":
    instrument_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
else:
    instrument_oauth2_scheme = lambda *args, **kwargs: None
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

instrument_server_tokens: Dict[float, dict] = {}

# Set up database engine
try:
    _url = url(security_config)
    engine = create_engine(_url)
except Exception:
    engine = None


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


"""
=======================================================================================
TOKEN VALIDATION FUNCTIONS
=======================================================================================

Functions and helpers used to validate incoming requests from both the client and
the frontend. 'validate_token()' and 'validate_instrument_token()' are imported
int the other FastAPI modules and attached as dependencies to the routers.
"""


def check_user(username: str) -> bool:
    try:
        with Session(engine) as murfey_db:
            users = murfey_db.exec(select(User)).all()
    except Exception:
        return False
    return username in [u.username for u in users]


async def validate_token(token: Annotated[str, Depends(oauth2_scheme)]):
    """
    Used by the backend routers to validate requests coming in from frontend.
    """
    try:
        # Validate using auth URL if provided; will error if invalid
        if auth_url:
            headers = (
                {}
                if security_config.auth_type == "cookie"
                else {"Authorization": f"Bearer {token}"}
            )
            cookies = (
                {security_config.cookie_key: token}
                if security_config.auth_type == "cookie"
                else {}
            )
            async with aiohttp.ClientSession(cookies=cookies) as session:
                async with session.get(
                    f"{auth_url}/validate_token",
                    headers=headers,
                ) as response:
                    success = response.status == 200
                    validation_outcome = await response.json()
            if not (success and validation_outcome.get("valid")):
                raise JWTError
        # If authenticating using cookies; an auth URL MUST be provided
        else:
            if security_config.auth_type == "cookie":
                raise JWTError
        # Validate using password
        if security_config.auth_type == "password":
            decoded_data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            # Check that the user is present and is valid
            if decoded_data.get("user"):
                if not check_user(decoded_data["user"]):
                    raise JWTError
            else:
                raise JWTError
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials from frontend",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return None


def validate_session_against_visit(session_id: int, visit: str):
    """
    Checks that the session ID is associated with the claimed visit.
    """
    with Session(engine) as murfey_db:
        session_data = murfey_db.exec(
            select(MurfeySession).where(MurfeySession.id == session_id)
        ).all()
    if len(session_data) != 1:
        return False
    return visit == session_data[0].visit


async def validate_instrument_token(
    token: Annotated[str, Depends(instrument_oauth2_scheme)]
):
    """
    Used by the backend routers to check the incoming instrument server token.
    """
    try:
        # Validate using auth URL if provided
        if security_config.instrument_auth_url:
            async with aiohttp.ClientSession() as session:
                headers = (
                    {}
                    if not security_config.instrument_auth_type
                    else {"Authorization": f"Bearer {token}"}
                )
                async with session.get(
                    f"{security_config.instrument_auth_url}/validate_token",
                    headers=headers,
                ) as response:
                    success = response.status == 200
                    validation_outcome = await response.json()
            if not (success and validation_outcome.get("valid")):
                raise JWTError
        else:
            # First, check if the token has expired
            decoded_data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            if expiry_time := decoded_data.get("expiry_time"):
                if expiry_time < time.time():
                    raise JWTError
            # Check that the decoded session corresponds to the visit
            elif decoded_data.get("session") is not None:
                if not validate_session_against_visit(
                    decoded_data["session"], decoded_data["visit"]
                ):
                    raise JWTError
            # Verify 'user' token if enabled
            elif security_config.allow_user_token:
                if not decoded_data.get("user"):
                    raise JWTError
            else:
                raise JWTError
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials from instrument",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return None


"""
=======================================================================================
SESSION ID VALIDATION
=======================================================================================

Annotated ints are defined here that trigger validation of the session IDs in incoming
requests, verifying that the session is allowed to access the particular visit.

The 'MurfeySessionID...' types are imported and used in the type hints of the endpoint
functions in the other FastAPI routers, depending on whether requests from the frontend
or the instrument are expected.
"""


def get_visit_name(session_id: int) -> str:
    with Session(engine) as murfey_db:
        return (
            murfey_db.exec(select(MurfeySession).where(MurfeySession.id == session_id))
            .one()
            .visit
        )


async def submit_to_auth_endpoint(url_subpath: str, token: str) -> None:
    if auth_url:
        headers = (
            {}
            if security_config.auth_type == "cookie"
            else {"Authorization": f"Bearer {token}"}
        )
        cookies = (
            {security_config.cookie_key: token}
            if security_config.auth_type == "cookie"
            else {}
        )
        async with aiohttp.ClientSession(cookies=cookies) as session:
            async with session.get(
                f"{auth_url}/{url_subpath}",
                headers=headers,
            ) as response:
                success = response.status == 200
                validation_outcome: dict = await response.json()
        if not (success and validation_outcome.get("valid")):
            logger.warning("Unauthorised visit access request from frontend")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="You do not have access to this visit",
                headers={"WWW-Authenticate": "Bearer"},
            )


async def validate_frontend_session_access(
    session_id: int,
    token: Annotated[str, Depends(oauth2_scheme)],
) -> int:
    """
    Validates whether a frontend request can access information about this session
    """
    visit_name = get_visit_name(session_id)
    await submit_to_auth_endpoint(f"validate_visit_access/{visit_name}", token)
    return session_id


async def validate_instrument_server_session_access(
    session_id: int,
    token: Annotated[str, Depends(instrument_oauth2_scheme)],
) -> int:
    """
    Validates whether an instrument request can access information about this session
    """
    visit_name = get_visit_name(session_id)

    if security_config.instrument_auth_url:
        async with aiohttp.ClientSession() as session:
            headers = (
                {}
                if not security_config.instrument_auth_type
                else {"Authorization": f"Bearer {token}"}
            )
            async with session.get(
                f"{security_config.instrument_auth_url}/validate_visit_access/{visit_name}",
                headers=headers,
            ) as response:
                success = response.status == 200
                validation_outcome = await response.json()
        if not (success and validation_outcome.get("valid")):
            logger.warning("Unauthorised visit access request from instrument")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="You do not have access to this visit",
                headers={"WWW-Authenticate": "Bearer"},
            )
    return session_id


async def validate_user_instrument_access(
    instrument_name: str,
    token: Annotated[str, Depends(oauth2_scheme)],
) -> str:
    """
    Validates whether a frontend request can access information about this instrument
    """
    await submit_to_auth_endpoint(
        f"validate_instrument_access/{instrument_name}", token
    )
    return instrument_name


# Set validation conditions for the session ID based on where the request is from
MurfeySessionIDFrontend = Annotated[int, Depends(validate_frontend_session_access)]
MurfeySessionIDInstrument = Annotated[
    int, Depends(validate_instrument_server_session_access)
]

MurfeyInstrumentNameFrontend = Annotated[str, Depends(validate_user_instrument_access)]


"""
=======================================================================================
API ENDPOINTS AND HELPER FUNCTIONS/CLASSES
=======================================================================================
"""


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def validate_user(username: str, password: str) -> bool:
    try:
        with Session(engine) as murfey_db:
            user = murfey_db.exec(select(User).where(User.username == username)).one()
    except Exception:
        return False
    return verify_password(password, user.hashed_password)


def create_access_token(data: dict, token: str = "") -> str:

    # If authenticating with password, auth URL needs a 'mint_session_token' endpoint
    if security_config.auth_type == "password":
        if auth_url and data.get("session"):
            session_id = data["session"]
            if not isinstance(session_id, int) and session_id > 0:
                # Check the session ID is alphanumeric for security
                raise ValueError("Session ID was invalid (not alphanumeric)")
            minted_token_response = requests.get(
                f"{auth_url}{url_path_for('auth.router', 'mint_session_token', session_id=session_id)}",
                headers={"Authorization": f"Bearer {token}"},
            )
            if minted_token_response.status_code != 200:
                raise RuntimeError(
                    f"Request received status code {minted_token_response.status_code} when trying to create session token"
                )
            return minted_token_response.json()["access_token"]

    to_encode = data.copy()

    # Make token for instrument
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


class Token(BaseModel):
    access_token: str
    token_type: str


@router.post("/token")
async def generate_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    # Only generate a token if it's a password
    if security_config.auth_type == "password":
        if auth_url:
            data = aiohttp.FormData()
            data.add_field("username", form_data.username)
            data.add_field("password", form_data.password)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{auth_url}{url_path_for('auth.router', 'generate_token')}",
                    data=data,
                ) as response:
                    validated = response.status == 200
                    token = await response.json()
                    access_token = token.get("access_token")
        else:
            validated = validate_user(form_data.username, form_data.password)
            access_token = create_access_token(
                data={"user": form_data.username},
            )
        if not validated:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return Token(access_token=access_token, token_type="bearer")

    # Return empty token otherwise
    return Token(access_token="", token_type="bearer")


@router.get("/sessions/{session_id}/token")
async def mint_session_token(session_id: MurfeySessionIDFrontend, db=murfey_db):
    visit = (
        db.exec(select(MurfeySession).where(MurfeySession.id == session_id)).one().visit
    )
    expiry_time = None
    if security_config.session_token_timeout:
        expiry_time = time.time() + security_config.session_token_timeout
    token = create_access_token(
        {
            "session": session_id,
            "visit": visit,
            "uuid": str(uuid4()),
            "expiry_time": expiry_time,
        }
    )
    return Token(access_token=token, token_type="bearer")


@router.get("/validate_token")
async def simple_token_validation(
    token: Annotated[str, Depends(validate_instrument_token)]
):
    return {"valid": True}
