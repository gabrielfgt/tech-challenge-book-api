"""
    Module responsible to define authentication methods for private routes
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.domain.user.model.user import User
import datetime
import jwt
import os


class JWTUtils:
    SECRET_KEY = os.getenv("JWT_SECRET")
    ALGORITHM = "HS256"

    @staticmethod
    def validate_token(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())) -> str:
        token = credentials.credentials

        try:
            jwt.decode(token, JWTUtils.SECRET_KEY, algorithms=[JWTUtils.ALGORITHM])
        
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="invalid token"
            )

        return token

    @staticmethod
    def admin_role(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())) -> str:
        token = credentials.credentials

        decoded = jwt.decode(token, JWTUtils.SECRET_KEY, algorithms=[JWTUtils.ALGORITHM])

        if decoded["role"] != "admin":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="you do not have admin permissions to access this resource"
            )

    @staticmethod
    def encode_jwt(payload: dict) -> str:
        return jwt.encode(payload, JWTUtils.SECRET_KEY, algorithm=JWTUtils.ALGORITHM)

    @staticmethod
    def decode_jwt(token: str) -> dict:
        return jwt.decode(token, JWTUtils.SECRET_KEY, algorithms=[JWTUtils.ALGORITHM])

    @staticmethod
    def generate_access_token(user: User, expiration_min: int) -> str:  
        payload = {
            "userId": user.id,
            "username": user.username,
            "role": user.role,
            "expires": int((datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=expiration_min)).timestamp())
        }

        return JWTUtils.encode_jwt(payload)

    @staticmethod
    def generate_refresh_token(user: User, expiration_min: int) -> str:
        payload = {
            "userId": user.id,
            "expires": int((datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=expiration_min)).timestamp())
        }   

        
        return JWTUtils.encode_jwt(payload)
