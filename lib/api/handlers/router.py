from fastapi import APIRouter

from lib.api.handlers import system, search, tracking, auth, oauth

api_router = APIRouter()

api_router.include_router(system.router)
api_router.include_router(auth.router)
api_router.include_router(oauth.router)
api_router.include_router(search.router)
api_router.include_router(tracking.router)
