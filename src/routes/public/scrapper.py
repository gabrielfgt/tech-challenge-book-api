from fastapi import APIRouter, BackgroundTasks, status
from fastapi.responses import JSONResponse
from src.scripts.scrapper_lib import trigger_scrap
from src.routes.public.scrapper_state import Scrapper

router = APIRouter()

@router.get("/scrapper")
async def trigger_scrapping(background_tasks: BackgroundTasks):    
    if Scrapper.getTaskState() == True:
        return {
            "message": "task is already running"
        } 
    
    
    background_tasks.add_task(trigger_scrap, Scrapper)

    Scrapper.setTaskState(True)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"message": "scrapper started in background"}
    )