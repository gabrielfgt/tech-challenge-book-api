from fastapi import APIRouter, Depends
from api.domain.auth.jwt_utils import JWTUtils

router = APIRouter()

@router.get("/admin", dependencies=[Depends(JWTUtils.validate_token), Depends(JWTUtils.admin_role)])
async def admin():
    return {"message": "You have access to this resource! 🚀🚀"}