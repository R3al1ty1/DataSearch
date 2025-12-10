from fastapi import APIRouter

from lib.api.handlers import system, search, tracking

api_router = APIRouter()

api_router.include_router(system.router)
api_router.include_router(search.router)
api_router.include_router(tracking.router)
