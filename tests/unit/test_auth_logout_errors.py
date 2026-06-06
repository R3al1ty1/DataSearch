import asyncio
from unittest.mock import Mock
from uuid import uuid4

from lib.auth.exceptions import TokenInvalid
from lib.auth.services.auth_service import AuthService


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


class DummyUnitOfWork:
    def __init__(self):
        self.security_events = DummySecurityEvents()


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
