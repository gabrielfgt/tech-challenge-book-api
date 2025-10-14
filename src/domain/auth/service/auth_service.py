import os

from fastapi import HTTPException, status
from src.domain.user.service.user_service import UserService
from src.domain.user.repository.impl.db_user_repository import DBUserRepository
from src.domain.user.repository.impl.memory_user_repository import InMemoryUserRepository
from src.domain.auth.repository.impl.database_auth_repository import DBAuthRepository
from src.domain.auth.repository.impl.memory_auth_repository import InMemoryAuthRepository
from src.domain.auth.service.jwt_utils import JWTUtils

class AuthService:

    user_service = None
    auth_repository = None


    def __init__(self):
        use_database = os.getenv("USE_DATABASE", "False")
        user_repository = DBUserRepository() if use_database == "True" else InMemoryUserRepository()

        self.user_service = UserService(user_repository)
        self.auth_repository = DBAuthRepository() if use_database == "True" else InMemoryAuthRepository()
    
    
    def renovate_access_token(self, credentials) -> dict:
        refresh_token = credentials.credentials.split(" ")[0]
        
        if refresh_token is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="any jwt token informed"
            )
        
        token_record = self.auth_repository.get_token_record_by_refresh_token(refresh_token=refresh_token)        

        if not token_record:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="you do not have permission to refresh irformed token"
            )
        
        user = self.user_service.get_user_by_id(user_id=token_record["user_id"])
        
        new_access_token = JWTUtils.generate_access_token(user=user, expiration_min=3)

        self.auth_repository.update_tokens(user.id, new_access_token)

        return {
            "accessToken": f"{new_access_token}",
            "refreshToken": f"{refresh_token}"
        }


    def generate_access_and_refresh_token(self, credentials):
        user = self.user_service.verify_user(credentials.username, credentials.password)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="you are not authorized to perform login."
            )   

        access_token = JWTUtils.generate_access_token(user=user, expiration_min=3)
        refresh_token = JWTUtils.generate_refresh_token(user=user, expiration_min=6)

        self.auth_repository.set_token(user.id, access_token=access_token, refresh_token=refresh_token)

        return {
            "accessToken": f"{access_token}",
            "refreshToken": f"{refresh_token}"
        }
    
    
        