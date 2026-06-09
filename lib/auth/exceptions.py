from typing import Any

from fastapi import status

from lib.core.error_codes import ErrorCode
from lib.core.exceptions import DataSearchError


class AuthenticationError(DataSearchError):
    def __init__(
        self,
        message: str = "Authentication failed",
        status_code: int = status.HTTP_401_UNAUTHORIZED,
        error_code: ErrorCode = ErrorCode.AUTHENTICATION_ERROR,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message, status_code, error_code, details, headers)


class MissingAuthHeader(AuthenticationError):
    def __init__(self) -> None:
        super().__init__(
            "Authorization header is required",
            error_code=ErrorCode.MISSING_AUTH_HEADER,
            headers={"WWW-Authenticate": "Bearer"},
        )


class MissingRefreshToken(AuthenticationError):
    def __init__(self) -> None:
        super().__init__("Refresh token is required", error_code=ErrorCode.MISSING_REFRESH_TOKEN)


class InvalidCredentials(AuthenticationError):
    def __init__(self) -> None:
        super().__init__("Invalid email or password", error_code=ErrorCode.INVALID_CREDENTIALS)


class TokenExpired(AuthenticationError):
    def __init__(self) -> None:
        super().__init__("Token has expired", error_code=ErrorCode.TOKEN_EXPIRED)


class TokenInvalid(AuthenticationError):
    def __init__(self) -> None:
        super().__init__("Invalid token", error_code=ErrorCode.TOKEN_INVALID)


class TokenBlacklisted(AuthenticationError):
    def __init__(self) -> None:
        super().__init__("Token has been revoked", error_code=ErrorCode.TOKEN_BLACKLISTED)


class UserNotFound(AuthenticationError):
    def __init__(self, identifier: str) -> None:
        super().__init__(
            "User not found",
            status.HTTP_404_NOT_FOUND,
            ErrorCode.USER_NOT_FOUND,
            {"identifier": identifier},
        )


class UserAlreadyExists(AuthenticationError):
    def __init__(self, email: str) -> None:
        super().__init__(
            "User with this email already exists",
            status.HTTP_409_CONFLICT,
            ErrorCode.USER_ALREADY_EXISTS,
            {"email": email},
        )


class PasswordValidationError(AuthenticationError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            status.HTTP_400_BAD_REQUEST,
            ErrorCode.PASSWORD_VALIDATION_FAILED,
        )


class RateLimitExceeded(AuthenticationError):
    def __init__(self, retry_after: int) -> None:
        super().__init__(
            f"Too many attempts. Try again in {retry_after} seconds",
            status.HTTP_429_TOO_MANY_REQUESTS,
            ErrorCode.RATE_LIMIT_EXCEEDED,
            {"retry_after": retry_after},
        )
        self.retry_after = retry_after


class AccountInactive(AuthenticationError):
    def __init__(self) -> None:
        super().__init__(
            "User account is inactive",
            status.HTTP_403_FORBIDDEN,
            ErrorCode.ACCOUNT_INACTIVE,
        )


class InsufficientPermissions(DataSearchError):
    def __init__(self, required_role: str) -> None:
        super().__init__(
            f"This action requires {required_role} role",
            status.HTTP_403_FORBIDDEN,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            {"required_role": required_role},
        )
