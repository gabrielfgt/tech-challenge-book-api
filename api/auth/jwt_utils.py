"""
    Module responsible to define authentication methods for private routes
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import datetime
import jwt


SECRET_KEY = "meu_secret_key"
ALGORITHM = "HS256"


def validate_token(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())) -> str:
    token = credentials.credentials

    try:
        jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="invalid token"
        )

    return token


def admin_role(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())) -> str:
    token = credentials.credentials

    decoded = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

    if decoded["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="you do not have admin permissions to access this resource"
        )


def encode_jwt(payload: dict) -> str:
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_jwt(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


def generate_access_token(user, expiration_min: int) -> str:
    payload = {
        "userId": user["id"],
        "username": user["username"],
        "role": user["role"],
        "expires": int((datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=expiration_min)).timestamp())
    }

    return encode_jwt(payload)


def generate_refresh_token(user, expiration_min: int) -> str:
    payload = {
        "userId": user["id"],
        "expires": int((datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=expiration_min)).timestamp())
    }   

    
    return encode_jwt(payload)
