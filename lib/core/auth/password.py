import re

from passlib.context import CryptContext

from lib.core.constants import AuthConstants
from lib.core.exceptions import PasswordValidationError

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    """Hash password using bcrypt."""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password against hash."""
    return pwd_context.verify(plain_password, hashed_password)


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
