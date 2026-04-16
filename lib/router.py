from fastapi import APIRouter

from lib.auth.router import router as auth_router
from lib.services.search.router import router as search_router
from lib.system.router import router as system_router

api_router = APIRouter()

api_router.include_router(auth_router)
api_router.include_router(search_router)
api_router.include_router(system_router)
