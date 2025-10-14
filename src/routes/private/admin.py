from fastapi import APIRouter, Depends
from src.domain.auth.service.jwt_utils import JWTUtils

router = APIRouter()

@router.get("/admin", dependencies=[Depends(JWTUtils.validate_token), Depends(JWTUtils.admin_role)])
async def admin():
    return {"message": "You have access to this resource! 🚀🚀"}