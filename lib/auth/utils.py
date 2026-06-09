import re
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import bcrypt
from authlib.integrations.starlette_client import OAuth
from jose import JWTError, jwt

from lib.auth.exceptions import PasswordValidationError, TokenExpired, TokenInvalid
from lib.core.config import Settings
from lib.core.constants import AuthConstants, UserRole


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
        "jti": str(uuid4()),
        "email": email,
        "role": role.value,
        "type": "access",
        "exp": expire,
        "iat": datetime.now(timezone.utc),
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
        "jti": str(uuid4()),
        "type": "refresh",
        "exp": expire,
        "iat": datetime.now(timezone.utc),
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
    """Get JTI from token for blacklist."""
    payload = decode_token(token, settings)
    return payload["jti"]

def get_token_expiry(token: str, settings: Settings) -> int:
    """Get seconds until token expiry."""
    payload = decode_token(token, settings)
    exp = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return max(int((exp - now).total_seconds()), 0)

oauth = OAuth()

def configure_oauth(settings: Settings) -> None:
    """Register OAuth providers. Called once at app startup."""
    if settings.GOOGLE_CLIENT_ID:
        oauth.register(
            name="google",
            client_id=settings.GOOGLE_CLIENT_ID,
            client_secret=settings.GOOGLE_CLIENT_SECRET,
            authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
            access_token_url="https://oauth2.googleapis.com/token",
            jwks_uri="https://www.googleapis.com/oauth2/v3/certs",
            userinfo_endpoint="https://openidconnect.googleapis.com/v1/userinfo",
            client_kwargs={"scope": "openid email profile"},
        )

    if settings.YANDEX_CLIENT_ID:
        oauth.register(
            name="yandex",
            client_id=settings.YANDEX_CLIENT_ID,
            client_secret=settings.YANDEX_CLIENT_SECRET,
            authorize_url="https://oauth.yandex.ru/authorize",
            access_token_url="https://oauth.yandex.ru/token",
            userinfo_endpoint="https://login.yandex.ru/info?format=json",
            client_kwargs={"scope": "login:email login:info"},
        )

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())

def validate_password(password: str) -> None:
    """
    Validate password against security requirements.

    Raises:
        PasswordValidationError: If password doesn't meet requirements
    """
    if len(password) < AuthConstants.PASSWORD_MIN_LENGTH:
        raise PasswordValidationError(
            f"Password must be at least {AuthConstants.PASSWORD_MIN_LENGTH} characters"
        )

    if len(password) > AuthConstants.PASSWORD_MAX_LENGTH:
        raise PasswordValidationError(
            f"Password must be less than {AuthConstants.PASSWORD_MAX_LENGTH} characters"
        )

    if AuthConstants.PASSWORD_REQUIRE_UPPERCASE and not re.search(r"[A-Z]", password):
        raise PasswordValidationError("Password must contain at least one uppercase letter")

    if AuthConstants.PASSWORD_REQUIRE_LOWERCASE and not re.search(r"[a-z]", password):
        raise PasswordValidationError("Password must contain at least one lowercase letter")

    if AuthConstants.PASSWORD_REQUIRE_DIGIT and not re.search(r"\d", password):
        raise PasswordValidationError("Password must contain at least one digit")

    if AuthConstants.PASSWORD_REQUIRE_SPECIAL and not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
        raise PasswordValidationError("Password must contain at least one special character")
