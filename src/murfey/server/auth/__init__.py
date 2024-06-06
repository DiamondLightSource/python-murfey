import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlmodel import Session, create_engine, select

from murfey.server.config import get_machine_config
from murfey.server.murfey_db import url
from murfey.util.db import MurfeyUser as User

machine_config = get_machine_config()
ALGORITHM = machine_config.auth_algorithm or "HS256"
SECRET_KEY = machine_config.auth_key or secrets.token_hex(32)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


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


def check_user(username: str) -> bool:
    try:
        with Session(engine) as murfey_db:
            users = murfey_db.exec(select(User)).all()
    except Exception:
        return False
    return username in [u.username for u in users]


async def validate_token(token: Annotated[str, Depends(oauth2_scheme)]):
    try:
        decoded_data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if not check_user(decoded_data.get("user")):
            raise JWTError
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return None
