from fastapi import APIRouter, Depends, Request, HTTPException, status
from api.auth.jwt_utils import validate_token, decode_jwt, admin_role

router = APIRouter()

@router.get("/api/private", dependencies=[Depends(validate_token)])
async def get_top_rated_books():
    return {"message": "You have access to this resource! 🚀🚀"}
