from fastapi import APIRouter
from lib.auth.router import router as auth_router
from lib.services.datasets.router import router as datasets_router
from lib.system.router import router as system_router

api_router = APIRouter()

api_router.include_router(auth_router)
api_router.include_router(datasets_router)
api_router.include_router(system_router)
