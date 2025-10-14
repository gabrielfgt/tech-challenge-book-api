from fastapi import FastAPI
from dotenv import load_dotenv
from src.routes.public import health, home, scrapper
from src.routes.private import private, admin
from src.routes.private import login
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

load_dotenv()
api = FastAPI()

api.include_router(home.router)
api.include_router(login.router)
api.include_router(health.router)
api.include_router(private.router)
api.include_router(admin.router)
api.include_router(scrapper.router)
