from __future__ import annotations

import importlib.metadata
import secrets
from logging import getLogger
from typing import Annotated, Dict

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlmodel import Session, create_engine, select

from murfey.server.murfey_db import url
from murfey.util.config import get_machine_config
from murfey.util.db import MurfeyUser as User
from murfey.util.db import Session as MurfeySession

# Set up logger
logger = getLogger("murfey.server.api.auth")

# Set up router
router = APIRouter()

# Set up variables used for authentication
machine_config = get_machine_config()
ALGORITHM = machine_config.auth_algorithm or "HS256"
SECRET_KEY = machine_config.auth_key or secrets.token_hex(32)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
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
    _url = url(get_machine_config())
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
    if validators := importlib.metadata.entry_points().select(
        group="murfey.auth.session_validation",
        name=machine_config.auth.session_validation,
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


async def validate_token(token: Annotated[str, Depends(oauth2_scheme)]):
    try:
        decoded_data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        # also validate against time stamps of successful instrument server connections
        if decoded_data.get("user"):
            if not check_user(decoded_data["user"]):
                raise JWTError
        elif decoded_data.get("timestamp"):
            if not validate_instrument_server_token(decoded_data["timestamp"]):
                raise JWTError
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


def create_access_token(data: dict):
    to_encode = data.copy()

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


"""
API ENDPOINTS
"""


@router.post("/token")
def generate_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    validated = validate_user(form_data.username, form_data.password)
    if not validated:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"user": form_data.username},
    )
    return Token(access_token=access_token, token_type="bearer")


@router.get("/validate_token")
async def simple_token_validation(token: Annotated[str, Depends(validate_token)]):
    return {"valid": True}
