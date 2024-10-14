from __future__ import annotations

import os
import secrets
import time
from logging import getLogger
from typing import Annotated, Dict
from uuid import uuid4

import aiohttp
import requests
from backports.entry_points_selectable import entry_points
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlmodel import Session, create_engine, select

from murfey.server import sanitise
from murfey.server.murfey_db import murfey_db, url
from murfey.util.config import get_machine_config, get_security_config
from murfey.util.db import MurfeyUser as User
from murfey.util.db import Session as MurfeySession

# Set up logger
logger = getLogger("murfey.server.api.auth")

# Set up router
router = APIRouter()


class CookieScheme(HTTPBearer):
    def __init__(
        self,
        *,
        description: str | None = None,
        auto_error: bool = True,
        cookie_key: str = "cookie_auth",
    ):
        """
        Args:
            cookie_key: Cookie key to look for in requests
        """
        super().__init__(
            description=description,
            auto_error=auto_error,
        )

        self.cookie_key = cookie_key

    async def __call__(self, request: Request):
        token = request.cookies.get(self.cookie_key)
        if token is None:
            if self.auto_error:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Not authenticated",
                )
            else:
                return None
        return token


# Set up variables used for authentication
security_config = get_security_config()
machine_config = get_machine_config()
auth_url = (
    machine_config[os.getenv("BEAMLINE", "")].auth_url
    if machine_config.get(os.getenv("BEAMLINE", ""))
    else ""
)
ALGORITHM = security_config.auth_algorithm or "HS256"
SECRET_KEY = security_config.auth_key or secrets.token_hex(32)
if security_config.auth_type == "password":
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
else:
    oauth2_scheme = CookieScheme(cookie_key=security_config.cookie_key)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

instrument_server_tokens: Dict[float, dict] = {}


"""
HELPER FUNCTIONS
"""


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


# Set up database engine
try:
    _url = url(security_config)
    engine = create_engine(_url)
except Exception:
    engine = None


def validate_user(username: str, password: str) -> bool:
    try:
        with Session(engine) as murfey_db:
            user = murfey_db.exec(select(User).where(User.username == username)).one()
    except Exception:
        return False
    return verify_password(password, user.hashed_password)


def validate_visit(visit_name: str, token: str) -> bool:
    if validators := entry_points().select(
        group="murfey.auth.session_validation",
        name=security_config.auth_type,
    ):
        return validators[0].load()(visit_name, token)
    return True


def check_user(username: str) -> bool:
    try:
        with Session(engine) as murfey_db:
            users = murfey_db.exec(select(User)).all()
    except Exception:
        return False
    return username in [u.username for u in users]


def validate_instrument_server_token(timestamp: float) -> bool:
    return timestamp in instrument_server_tokens.keys()


def validate_instrument_server_session_token(session_id: int, visit: str):
    with Session(engine) as murfey_db:
        session_data = murfey_db.exec(
            select(MurfeySession).where(MurfeySession.id == session_id)
        ).all()
    if len(session_data) != 1:
        return False
    return visit == session_data[0].visit


def password_token_validation(token: str):
    decoded_data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    # first check if the token has expired
    if expiry_time := decoded_data.get("expiry_time"):
        if expiry_time < time.time():
            raise JWTError
    if decoded_data.get("user"):
        if not check_user(decoded_data["user"]):
            raise JWTError
    elif decoded_data.get("session") is not None:
        if not validate_instrument_server_session_token(
            decoded_data["session"], decoded_data["visit"]
        ):
            raise JWTError
    else:
        raise JWTError


async def validate_token(token: Annotated[str, Depends(oauth2_scheme)]):
    try:
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
        else:
            if validators := entry_points().select(
                group="murfey.auth.token_validation",
                name=security_config.auth_type,
            ):
                validators[0].load()(token)
            else:
                raise JWTError
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return None


async def validate_session_access(
    session_id: int, token: Annotated[str, Depends(oauth2_scheme)]
) -> int:
    await validate_token(token)
    with Session(engine) as murfey_db:
        visit_name = (
            murfey_db.exec(select(MurfeySession).where(MurfeySession.id == session_id))
            .one()
            .visit
        )
    if not validate_visit(visit_name, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You do not have access to this visit",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return session_id


class Token(BaseModel):
    access_token: str
    token_type: str


def create_access_token(data: dict, token: str = "") -> str:
    if auth_url and data.get("session"):
        session_id = data["session"]
        if not isinstance(session_id, int) and session_id > 0:
            # check the session ID is alphanumeric for security
            raise ValueError("Session ID was invalid (not alphanumeric)")
        minted_token_response = requests.get(
            f"{auth_url}/sessions/{sanitise(str(session_id))}/token",
            headers={"Authorization": f"Bearer {token}"},
        )
        if minted_token_response.status_code != 200:
            raise RuntimeError(
                f"Request received status code {minted_token_response.status_code} when trying to create session token"
            )
        return minted_token_response.json()["access_token"]

    to_encode = data.copy()

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


MurfeySessionID = Annotated[int, Depends(validate_session_access)]

"""
API ENDPOINTS
"""


@router.post("/token")
async def generate_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    if auth_url:
        data = aiohttp.FormData()
        data.add_field("username", form_data.username)
        data.add_field("password", form_data.password)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{auth_url}/token",
                data=data,
            ) as response:
                validated = response.status == 200
                token = await response.json()
                access_token = token.get("access_token")
    else:
        validated = validate_user(form_data.username, form_data.password)
    if not validated:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not auth_url:
        access_token = create_access_token(
            data={"user": form_data.username},
        )
    return Token(access_token=access_token, token_type="bearer")


@router.get("/sessions/{session_id}/token")
async def mint_session_token(session_id: MurfeySessionID, db=murfey_db):
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
async def simple_token_validation(token: Annotated[str, Depends(validate_token)]):
    return {"valid": True}
