from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPAuthorizationCredentials, HTTPBearer
from api.users.user import verify_user, get_user_by_id
from api.auth.jwt_utils import decode_jwt, generate_refresh_token, generate_access_token
from api.auth.auth_db import update_tokens

router = APIRouter()
basic_auth = HTTPBasic()
jwt_auth = HTTPBearer()

@router.post("/auth/login")
async def get_tokens(credentials: HTTPBasicCredentials = Depends(basic_auth)):
    user = verify_user(credentials.username, credentials.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="you are not authorized to perform login."
        )   

    access_token = generate_access_token(user, 3)
    refresh_token = generate_refresh_token(user, 6)

    return {
        "accessToken": f"{access_token}",
        "refreshToken": f"{refresh_token}"
    }


@router.post("/auth/refresh")
async def refresh_token(credentials: HTTPAuthorizationCredentials = Depends(jwt_auth)):
    refresh_token = credentials.credentials.split(" ")[0]
    
    if refresh_token is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="invalid refresh token"
        )

    decoded_token = decode_jwt(refresh_token)
    print(decoded_token)
    user_id = decoded_token["userId"]

    user = get_user_by_id(user_id)

    new_access_token = generate_access_token(user, 3)

    update_tokens(user_id, new_access_token)

    return {
        "token": f"{new_access_token}",
        "refresh": f"{refresh_token}"
    }



