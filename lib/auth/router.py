import logging
from typing import Annotated
from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession
from lib.core.container import container
from lib.core.exceptions import AuthenticationError
from lib.auth.schemas import (
    RegisterRequest,
    LoginRequest,
    AuthResponse,
    TokenResponse,
    UserResponse
)
from lib.auth.dependencies import (
    get_current_active_user,
    get_ip_address,
    get_user_agent,
    set_refresh_cookie,
    delete_refresh_cookie,
)
from lib.auth.models import User
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from lib.auth.utils import oauth
from lib.auth.schemas import TokenResponse, YandexTokenRequest
from lib.auth.dependencies import get_ip_address, get_user_agent, set_refresh_cookie

router = APIRouter(tags=["Auth"])

@router.post("/auth/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
async def register(
    request: Request,
    response: Response,
    body: RegisterRequest,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    try:
        result = await container.auth_service.register(
            session=db,
            email=body.email,
            password=body.password,
            full_name=body.full_name,
            ip_address=get_ip_address(request),
            user_agent=get_user_agent(request),
        )
        set_refresh_cookie(response, result.refresh_token)
        return AuthResponse(
            user=UserResponse.model_validate(result.user),
            access_token=result.access_token,
        )
    except AuthenticationError as e:
        logger.warning(f"Registration failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.post("/auth/login", response_model=AuthResponse)
async def login(
    request: Request,
    response: Response,
    body: LoginRequest,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    try:
        result = await container.auth_service.login(
            session=db,
            email=body.email,
            password=body.password,
            ip_address=get_ip_address(request),
            user_agent=get_user_agent(request),
        )
        set_refresh_cookie(response, result.refresh_token)
        return AuthResponse(
            user=UserResponse.model_validate(result.user),
            access_token=result.access_token,
        )
    except AuthenticationError as e:
        logger.warning(f"Login failed: {e}")
        if "rate limit" in str(e).lower():
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(e))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

@router.post("/auth/refresh", response_model=TokenResponse)
async def refresh_token(
    response: Response,
    refresh_token: str | None = Cookie(None),
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token missing")
    try:
        tokens = await container.auth_service.refresh_token(
            session=db,
            refresh_token=refresh_token,
        )
        set_refresh_cookie(response, tokens.refresh_token)
        return TokenResponse(access_token=tokens.access_token)
    except AuthenticationError as e:
        logger.warning(f"Token refresh failed: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    response: Response,
    current_user: Annotated[User, Depends(get_current_active_user)],
    refresh_token: str | None = Cookie(None),
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()

    await container.auth_service.logout(
        session=db,
        access_token=token,
        refresh_token=refresh_token,
        user_id=current_user.id,
        ip_address=get_ip_address(request),
        user_agent=get_user_agent(request),
    )
    delete_refresh_cookie(response)

@router.get("/auth/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: Annotated[User, Depends(get_current_active_user)]
):
    return UserResponse.model_validate(current_user)

@router.get("/auth/oauth/google", include_in_schema=False)
async def google_oauth_init(request: Request) -> RedirectResponse:
    redirect_uri = f"{container.settings.OAUTH_CALLBACK_BASE_URL}{container.settings.API_V1_STR}/auth/oauth/google/callback"
    return await oauth.google.authorize_redirect(request, redirect_uri)

@router.get("/auth/oauth/google/callback", include_in_schema=False)
async def google_oauth_callback(
    request: Request,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
) -> RedirectResponse:
    try:
        token = await oauth.google.authorize_access_token(request)
        userinfo = token.get("userinfo") or await oauth.google.userinfo(token=token)

        result = await container.oauth_service.oauth_login_or_register(
            session=db,
            provider="google",
            provider_id=str(userinfo["sub"]),
            email=userinfo["email"],
            full_name=userinfo.get("name"),
            ip_address=get_ip_address(request),
            user_agent=get_user_agent(request),
        )
    except AuthenticationError as e:
        logger.error(f"Google OAuth failed: {e}")
        return RedirectResponse(url=f"{container.settings.FRONTEND_URL}/auth/error")

    redirect = RedirectResponse(url=f"{container.settings.FRONTEND_URL}/dashboard")
    set_refresh_cookie(redirect, result.refresh_token)
    redirect.headers["X-Access-Token"] = result.access_token
    return redirect

@router.get("/auth/oauth/yandex", include_in_schema=False)
async def yandex_oauth_init(request: Request) -> RedirectResponse:
    redirect_uri = f"{container.settings.OAUTH_CALLBACK_BASE_URL}{container.settings.API_V1_STR}/auth/oauth/yandex/callback"
    return await oauth.yandex.authorize_redirect(request, redirect_uri)

@router.get("/auth/oauth/yandex/callback", include_in_schema=False)
async def yandex_oauth_callback(
    request: Request,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
) -> RedirectResponse:
    try:
        token = await oauth.yandex.authorize_access_token(request)
        userinfo = await oauth.yandex.userinfo(token=token)
        email: str = userinfo.get("default_email") or userinfo["emails"][0]

        result = await container.oauth_service.oauth_login_or_register(
            session=db,
            provider="yandex",
            provider_id=str(userinfo["id"]),
            email=email,
            full_name=userinfo.get("display_name"),
            ip_address=get_ip_address(request),
            user_agent=get_user_agent(request),
        )
    except AuthenticationError as e:
        logger.error(f"Yandex OAuth failed: {e}")
        return RedirectResponse(url=f"{container.settings.FRONTEND_URL}/auth/error")

    redirect = RedirectResponse(url=f"{container.settings.FRONTEND_URL}/dashboard")
    set_refresh_cookie(redirect, result.refresh_token)
    redirect.headers["X-Access-Token"] = result.access_token
    return redirect

@router.post("/auth/oauth/yandex/token", response_model=TokenResponse)
async def yandex_token_login(
    body: YandexTokenRequest,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
) -> TokenResponse:
    try:
        result = await container.oauth_service.yandex_token_login(
            session=db,
            yandex_token=body.yandex_token,
            ip_address=get_ip_address(request),
            user_agent=get_user_agent(request),
        )
    except AuthenticationError as e:
        logger.error(f"Yandex token login failed: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

    set_refresh_cookie(response, result.refresh_token)
    return TokenResponse(access_token=result.access_token)
