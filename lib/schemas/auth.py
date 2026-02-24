from uuid import UUID
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, ConfigDict

from lib.core.constants import UserRole


class RegisterRequest(BaseModel):
    """Request for user registration."""
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)
    full_name: str | None = None


class LoginRequest(BaseModel):
    """Request for user login."""
    email: EmailStr
    password: str


class RefreshTokenRequest(BaseModel):
    """Request for refreshing access token."""
    refresh_token: str


class UserResponse(BaseModel):
    """Response with user information."""
    id: UUID
    email: str
    full_name: str | None
    role: UserRole
    is_active: bool
    is_email_verified: bool
    last_login_at: datetime | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TokenResponse(BaseModel):
    """Response with JWT tokens."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class AuthResponse(BaseModel):
    """Response with user and tokens."""
    user: UserResponse
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class OAuthUrlResponse(BaseModel):
    """Response with OAuth authorization URL."""
    auth_url: str
    state: str
