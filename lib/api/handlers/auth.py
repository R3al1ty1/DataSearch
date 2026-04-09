import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container
from lib.core.exceptions import AuthenticationError
from lib.schemas.auth import (
    RegisterRequest,
    LoginRequest,
    RefreshTokenRequest,
    AuthResponse,
    TokenResponse,
    UserResponse
)
from lib.api.dependencies.auth import (
    get_current_active_user,
    get_ip_address,
    get_user_agent
)
from lib.models.user import User


router = APIRouter(tags=["Authentication"])


@router.post("/auth/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
async def register(
    request: Request,
    body: RegisterRequest,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Register new user."""
    try:
        ip_address = get_ip_address(request)
        user_agent = get_user_agent(request)

        result = await container.auth_service.register(
            session=db,
            email=body.email,
            password=body.password,
            full_name=body.full_name,
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
        logger.warning(f"Registration failed: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/auth/login", response_model=AuthResponse)
async def login(
    request: Request,
    body: LoginRequest,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Login user."""
    try:
        ip_address = get_ip_address(request)
        user_agent = get_user_agent(request)

        result = await container.auth_service.login(
            session=db,
            email=body.email,
            password=body.password,
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
        logger.warning(f"Login failed: {e}")
        if "rate limit" in str(e).lower():
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(e))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))


@router.post("/auth/refresh", response_model=TokenResponse)
async def refresh_token(
    body: RefreshTokenRequest,
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Refresh access token."""
    try:
        tokens = await container.auth_service.refresh_token(
            session=db,
            refresh_token=body.refresh_token
        )

        return TokenResponse(
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type
        )
    except AuthenticationError as e:
        logger.warning(f"Token refresh failed: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
):
    """Logout user."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")

    ip_address = get_ip_address(request)
    user_agent = get_user_agent(request)

    await container.auth_service.logout(
        session=db,
        access_token=token,
        user_id=current_user.id,
        ip_address=ip_address,
        user_agent=user_agent
    )


@router.get("/auth/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: Annotated[User, Depends(get_current_active_user)]
):
    """Get current user information."""
    return UserResponse.model_validate(current_user)
