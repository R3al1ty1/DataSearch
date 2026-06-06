from datetime import datetime, timedelta, timezone
from uuid import UUID

from jose import JWTError, jwt

from lib.auth.exceptions import TokenExpired, TokenInvalid
from lib.core.config import Settings
from lib.core.constants import UserRole


def create_access_token(
    user_id: UUID,
    email: str,
    role: UserRole,
    settings: Settings
) -> str:
    """Create JWT access token."""
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
    )

    payload = {
        "sub": str(user_id),
        "email": email,
        "role": role.value,
        "type": "access",
        "exp": expire,
        "iat": datetime.now(timezone.utc)
    }

    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(
    user_id: UUID,
    settings: Settings
) -> str:
    """Create JWT refresh token."""
    expire = datetime.now(timezone.utc) + timedelta(
        days=settings.REFRESH_TOKEN_EXPIRE_DAYS
    )

    payload = {
        "sub": str(user_id),
        "type": "refresh",
        "exp": expire,
        "iat": datetime.now(timezone.utc)
    }

    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str, settings: Settings) -> dict:
    """
    Decode and validate JWT token.

    Raises:
        TokenExpired: If token has expired
        TokenInvalid: If token is malformed or invalid
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise TokenExpired()
    except JWTError:
        raise TokenInvalid()


def get_token_jti(token: str, settings: Settings) -> str:
    """Get JTI (JWT ID) from token for blacklist."""
    payload = decode_token(token, settings)
    return f"{payload['sub']}:{payload['iat']}"


def get_token_expiry(token: str, settings: Settings) -> int:
    """Get seconds until token expiry."""
    payload = decode_token(token, settings)
    exp = datetime.fromtimestamp(payload['exp'], tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return max(int((exp - now).total_seconds()), 0)
