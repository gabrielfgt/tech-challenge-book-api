from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
async def healt_check():
    return {"message": "ok"}