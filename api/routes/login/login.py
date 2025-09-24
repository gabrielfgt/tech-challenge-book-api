from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPAuthorizationCredentials, HTTPBearer
from api.domain.users.service import UserService
from api.domain.users.memory_repository import InMemoryUserRepository
from api.domain.auth.jwt_utils import JWTUtils
import api.domain.auth.auth_db as AuthDB
from api.domain.login.service import LoginService

router = APIRouter()
basic_auth = HTTPBasic()
jwt_auth = HTTPBearer()

@router.post("/auth/login")
async def get_api_token(credentials: HTTPBasicCredentials = Depends(basic_auth)):
    userService = UserService(InMemoryUserRepository("conn"))
    loginService = LoginService(user_service=userService, auth_db=AuthDB)

    return loginService.generate_access_and_refresh_token(username=credentials.username, password=credentials.password)
    


@router.post("/auth/refresh")
async def refresh_api_token(credentials: HTTPAuthorizationCredentials = Depends(jwt_auth)):
    refresh_token = credentials.credentials.split(" ")[0]
    
    userService = UserService(InMemoryUserRepository("conn"))
    loginService = LoginService(user_service=userService, auth_db=AuthDB)

    return loginService.renovate_access_token(refresh_token=refresh_token)


