from typing import Annotated

from fastapi import Depends, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from lib.auth.exceptions import AccountInactive, InsufficientPermissions, MissingAuthHeader
from lib.auth.models import User
from lib.core.constants import UserRole
from lib.core.container import container
from lib.core.uow import UnitOfWork

security = HTTPBearer(auto_error=False)

async def get_uow():
    async with container.uow() as uow:
        yield uow

async def get_current_user(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    uow: UnitOfWork = Depends(get_uow, scope="function")
) -> User:
    """Dependency to get current authenticated user."""
    if credentials is None:
        raise MissingAuthHeader()
    return await container.auth_service.get_current_user(uow, credentials.credentials)

async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)]
) -> User:
    """Dependency to get current active user."""
    if not current_user.is_active:
        raise AccountInactive()
    return current_user

def require_role(required_role: UserRole):
    """Dependency factory for role-based authorization."""
    async def check_role(
        current_user: Annotated[User, Depends(get_current_active_user)]
    ) -> User:
        user_role = UserRole(current_user.role)
        if user_role == UserRole.ADMIN:
            return current_user
        if user_role != required_role:
            raise InsufficientPermissions(required_role.value)
        return current_user

    return check_role

def get_ip_address(request: Request) -> str | None:
    """Extract IP address from request."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else None

def get_user_agent(request: Request) -> str | None:
    """Extract User-Agent from request."""
    return request.headers.get("User-Agent")

def set_refresh_cookie(response: Response, token: str) -> None:
    """Set refresh token as HttpOnly cookie."""
    response.set_cookie(
        key="refresh_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=container.settings.REFRESH_TOKEN_EXPIRE_DAYS * 86400,
        secure=container.settings.ENVIRONMENT != "local",
    )

def delete_refresh_cookie(response: Response) -> None:
    """Delete refresh token cookie."""
    response.delete_cookie(
        key="refresh_token",
        httponly=True,
        samesite="lax",
    )
