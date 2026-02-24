import logging

from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container
from lib.core.exceptions import AuthenticationError
from lib.schemas.auth import OAuthUrlResponse, AuthResponse, UserResponse
from lib.api.dependencies.auth import get_ip_address, get_user_agent


router = APIRouter(tags=["OAuth"])


@router.get("/auth/oauth/google", response_model=OAuthUrlResponse)
async def google_oauth_init(
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Initiate Google OAuth flow."""
    try:
        state = await container.oauth_service.generate_oauth_state()
        auth_url = container.oauth_service.get_google_auth_url(state)

        return OAuthUrlResponse(auth_url=auth_url, state=state)
    except AuthenticationError as e:
        logger.error(f"Google OAuth init failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("/auth/oauth/google/callback", response_model=AuthResponse)
async def google_oauth_callback(
    request: Request,
    code: str = Query(...),
    state: str = Query(...),
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Google OAuth callback."""
    try:
        if not await container.oauth_service.verify_oauth_state(state):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state")

        user_info = await container.oauth_service.exchange_google_code(code)

        ip_address = get_ip_address(request)
        user_agent = get_user_agent(request)

        result = await container.oauth_service.oauth_login_or_register(
            session=db,
            provider="google",
            provider_id=user_info.provider_id,
            email=user_info.email,
            full_name=user_info.full_name,
            ip_address=ip_address,
            user_agent=user_agent
        )

        return AuthResponse(
            user=UserResponse.model_validate(result.user),
            access_token=result.access_token,
            refresh_token=result.refresh_token,
            token_type=result.token_type
        )
    except AuthenticationError as e:
        logger.error(f"Google OAuth callback failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("/auth/oauth/yandex", response_model=OAuthUrlResponse)
async def yandex_oauth_init(
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Initiate Yandex OAuth flow."""
    try:
        state = await container.oauth_service.generate_oauth_state()
        auth_url = container.oauth_service.get_yandex_auth_url(state)

        return OAuthUrlResponse(auth_url=auth_url, state=state)
    except AuthenticationError as e:
        logger.error(f"Yandex OAuth init failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("/auth/oauth/yandex/callback", response_model=AuthResponse)
async def yandex_oauth_callback(
    request: Request,
    code: str = Query(...),
    state: str = Query(...),
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Yandex OAuth callback."""
    try:
        if not await container.oauth_service.verify_oauth_state(state):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state")

        user_info = await container.oauth_service.exchange_yandex_code(code)

        ip_address = get_ip_address(request)
        user_agent = get_user_agent(request)

        result = await container.oauth_service.oauth_login_or_register(
            session=db,
            provider="yandex",
            provider_id=user_info.provider_id,
            email=user_info.email,
            full_name=user_info.full_name,
            ip_address=ip_address,
            user_agent=user_agent
        )

        return AuthResponse(
            user=UserResponse.model_validate(result.user),
            access_token=result.access_token,
            refresh_token=result.refresh_token,
            token_type=result.token_type
        )
    except AuthenticationError as e:
        logger.error(f"Yandex OAuth callback failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
