from fastapi import APIRouter, Depends, Request, HTTPException, status
from api.auth.jwt_utils import validate_token, admin_role

router = APIRouter()

@router.get("/admin", dependencies=[Depends(validate_token), Depends(admin_role)])
async def admin():
    return {"message": "You have access to this resource! 🚀🚀"}