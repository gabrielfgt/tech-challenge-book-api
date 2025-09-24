from fastapi import APIRouter, Depends
from api.domain.auth.jwt_utils import JWTUtils

router = APIRouter()

@router.get("/api/private", dependencies=[Depends(JWTUtils.validate_token)])
async def get_top_rated_books():
    return {"message": "You have access to this resource! 🚀🚀"}
