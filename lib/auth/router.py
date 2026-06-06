import logging
from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, Request, Response, status
from fastapi.responses import RedirectResponse

from lib.auth.dependencies import (
    delete_refresh_cookie,
    get_current_active_user,
    get_ip_address,
    get_uow,
    get_user_agent,
    set_refresh_cookie,
)
from lib.auth.exceptions import AuthenticationError, MissingRefreshToken
from lib.auth.models import User
from lib.auth.schemas import (
    AuthResponse,
    LoginRequest,
    RegisterRequest,
    TokenResponse,
    UserResponse,
    YandexTokenRequest,
)
from lib.auth.utils import oauth
from lib.core.container import container
from lib.core.openapi import COMMON_ERROR_RESPONSES
from lib.core.uow import UnitOfWork

router = APIRouter(tags=["Auth"], responses=COMMON_ERROR_RESPONSES)

@router.post("/auth/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
async def register(
    request: Request,
    response: Response,
    body: RegisterRequest,
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    result = await container.auth_service.register(
        uow=uow,
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

@router.post("/auth/login", response_model=AuthResponse)
async def login(
    request: Request,
    response: Response,
    body: LoginRequest,
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    result = await container.auth_service.login(
        uow=uow,
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

@router.post("/auth/refresh", response_model=TokenResponse)
async def refresh_token(
    response: Response,
    refresh_token: str | None = Cookie(None),
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    if not refresh_token:
        raise MissingRefreshToken()
    tokens = await container.auth_service.refresh_token(
        uow=uow,
        refresh_token=refresh_token,
    )
    set_refresh_cookie(response, tokens.refresh_token)
    return TokenResponse(access_token=tokens.access_token)

@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    response: Response,
    current_user: Annotated[User, Depends(get_current_active_user)],
    refresh_token: str | None = Cookie(None),
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()

    await container.auth_service.logout(
        uow=uow,
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
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
) -> RedirectResponse:
    try:
        token = await oauth.google.authorize_access_token(request)
        userinfo = token.get("userinfo") or await oauth.google.userinfo(token=token)

        result = await container.oauth_service.oauth_login_or_register(
            uow=uow,
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
    return redirect

@router.get("/auth/oauth/yandex", include_in_schema=False)
async def yandex_oauth_init(request: Request) -> RedirectResponse:
    redirect_uri = f"{container.settings.OAUTH_CALLBACK_BASE_URL}{container.settings.API_V1_STR}/auth/oauth/yandex/callback"
    return await oauth.yandex.authorize_redirect(request, redirect_uri)

@router.get("/auth/oauth/yandex/callback", include_in_schema=False)
async def yandex_oauth_callback(
    request: Request,
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
) -> RedirectResponse:
    try:
        token = await oauth.yandex.authorize_access_token(request)
        userinfo = await oauth.yandex.userinfo(token=token)
        email: str = userinfo.get("default_email") or userinfo["emails"][0]

        result = await container.oauth_service.oauth_login_or_register(
            uow=uow,
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
    return redirect

@router.post("/auth/oauth/yandex/token", response_model=TokenResponse)
async def yandex_token_login(
    body: YandexTokenRequest,
    request: Request,
    response: Response,
    uow: UnitOfWork = Depends(get_uow, scope="function"),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
) -> TokenResponse:
    result = await container.oauth_service.yandex_token_login(
        uow=uow,
        yandex_token=body.yandex_token,
        ip_address=get_ip_address(request),
        user_agent=get_user_agent(request),
    )

    set_refresh_cookie(response, result.refresh_token)
    return TokenResponse(access_token=result.access_token)
