
from fastapi import HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials
from api.domain.users.service import UserService
from api.domain.auth.jwt_utils import JWTUtils

class LoginService:
    def __init__(self, user_service: UserService, auth_db: any):
        self.user_service = user_service
        self.auth_db = auth_db      


    def generate_access_and_refresh_token(self, username: str, password: str) -> dict:        
        user = self.user_service.verify_user(username, password)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="you are not authorized to perform login."
            )   

        access_token = JWTUtils.generate_access_token(user=user, expiration_min=3)
        refresh_token = JWTUtils.generate_refresh_token(user=user, expiration_min=6)

        self.auth_db.set_token(user.id, access_token=access_token, refresh_token=refresh_token)

        return {
            "accessToken": f"{access_token}",
            "refreshToken": f"{refresh_token}"
        }
    

    def renovate_access_token(self, refresh_token: str) -> dict:
        if refresh_token is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="any jwt token informed"
            )
        
        token_record = self.auth_db.get_token_record_by_refresh_token(refresh_token=refresh_token)

        if not token_record:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="you do not have permission to refresh irformed token"
            )
        
        user = self.user_service.get_user_by_id(user_id=token_record["user_id"])
        
        new_access_token = JWTUtils.generate_access_token(user=user, expiration_min=3)

        self.auth_db.update_tokens(user.id, new_access_token)

        return {
            "accessToken": f"{new_access_token}",
            "refreshToken": f"{refresh_token}"
        }
