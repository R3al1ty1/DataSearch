import asyncio
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

from lib.auth.models import User
from lib.auth.exceptions import TokenInvalid
from lib.auth.services.auth_service import AuthService
from lib.auth.utils import hash_password
from lib.core.constants import UserRole


class DummyTokenService:
    def __init__(self):
        self.calls = []

    async def blacklist_token(self, token: str) -> None:
        self.calls.append(token)
        if token == "bad-refresh":
            raise TokenInvalid()


class DummySecurityEvents:
    def __init__(self):
        self.logged = False

    async def log_event(self, **kwargs):
        self.logged = True


class DummyUsers:
    def __init__(self, user: User):
        self.user = user
        self.last_login_at = datetime.now(UTC).replace(tzinfo=None)

    async def get_by_email(self, email: str) -> User:
        return self.user

    async def update_last_login(self, user_id):
        return self.last_login_at


class DummyUnitOfWork:
    def __init__(self, user: User | None = None):
        if user is not None:
            self.users = DummyUsers(user)
        self.security_events = DummySecurityEvents()


class DummyRateLimitService:
    async def check_rate_limit(self, email: str) -> None:
        return None

    async def reset_attempts(self, email: str) -> None:
        return None


class DummyLoginTokenService(DummyTokenService):
    def generate_tokens(self, user: User):
        return Mock(
            access_token="access",
            refresh_token="refresh",
            token_type="bearer",
        )


def make_user() -> User:
    now = datetime.now(UTC)
    return User(
        id=uuid4(),
        email="user@example.com",
        password_hash=hash_password("password"),
        full_name=None,
        role=UserRole.USER.value,
        is_active=True,
        is_email_verified=True,
        created_at=now,
        updated_at=now,
    )


def test_logout_logs_refresh_token_parse_failure_and_continues():
    token_service = DummyTokenService()
    logger = Mock()
    service = AuthService(
        token_service=token_service,
        rate_limit_service=Mock(),
        settings=Mock(),
        logger=logger,
    )
    uow = DummyUnitOfWork()

    asyncio.run(
        service.logout(
            uow=uow,
            access_token="access",
            refresh_token="bad-refresh",
            user_id=uuid4(),
        )
    )

    assert token_service.calls == ["access", "bad-refresh"]
    logger.warning.assert_called_once()
    assert uow.security_events.logged is True


def test_login_updates_user_last_login_in_memory():
    user = make_user()
    service = AuthService(
        token_service=DummyLoginTokenService(),
        rate_limit_service=DummyRateLimitService(),
        settings=Mock(),
        logger=Mock(),
    )
    uow = DummyUnitOfWork(user)

    result = asyncio.run(
        service.login(
            uow=uow,
            email=user.email,
            password="password",
        )
    )

    assert result.user.last_login_at == uow.users.last_login_at
