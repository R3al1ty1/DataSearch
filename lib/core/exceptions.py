class DataSearchBaseException(Exception):
    """Base exception for DataSearch application."""
    pass

class ResourceNotFound(DataSearchBaseException):
    def __init__(self, resource: str, identifier: str):
        self.message = f"{resource} with id '{identifier}' not found."
        super().__init__(self.message)

class ExternalServiceError(DataSearchBaseException):
    def __init__(self, service: str, details: str):
        self.message = f"Error communicating with {service}: {details}"
        super().__init__(self.message)

class InvalidSearchQuery(DataSearchBaseException):
    pass

class AuthenticationError(DataSearchBaseException):
    """Base authentication error."""
    pass

class InvalidCredentials(AuthenticationError):
    """Invalid email or password."""
    def __init__(self):
        self.message = "Invalid email or password"
        super().__init__(self.message)

class TokenExpired(AuthenticationError):
    """JWT token has expired."""
    def __init__(self):
        self.message = "Token has expired"
        super().__init__(self.message)

class TokenInvalid(AuthenticationError):
    """JWT token is invalid."""
    def __init__(self):
        self.message = "Invalid token"
        super().__init__(self.message)

class TokenBlacklisted(AuthenticationError):
    """Token has been revoked."""
    def __init__(self):
        self.message = "Token has been revoked"
        super().__init__(self.message)

class UserNotFound(AuthenticationError):
    """User not found."""
    def __init__(self, identifier: str):
        self.message = f"User {identifier} not found"
        super().__init__(self.message)

class UserAlreadyExists(AuthenticationError):
    """User with this email already exists."""
    def __init__(self, email: str):
        self.message = f"User with email {email} already exists"
        super().__init__(self.message)

class PasswordValidationError(AuthenticationError):
    """Password does not meet requirements."""
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

class RateLimitExceeded(AuthenticationError):
    """Too many attempts, rate limit exceeded."""
    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        self.message = f"Too many attempts. Try again in {retry_after} seconds"
        super().__init__(self.message)

class InsufficientPermissions(DataSearchBaseException):
    """User lacks required permissions."""
    def __init__(self, required_role: str):
        self.message = f"This action requires {required_role} role"
        super().__init__(self.message)
