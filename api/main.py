from fastapi import FastAPI

from api.routes.public import health
from api.routes.private import private, admin
from api.routes.login import login

api = FastAPI()

api.include_router(login.router)
api.include_router(health.router)
api.include_router(private.router)
api.include_router(admin.router)
