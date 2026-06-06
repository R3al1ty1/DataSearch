import logging

import httpx

from lib.auth.services.auth_service import AuthResult
from lib.auth.services.token_service import TokenService
from lib.core.constants import UserRole
from lib.core.exceptions import AuthenticationError
from lib.core.uow import UnitOfWork


class OAuthService:
    """Service for OAuth authentication (Google, Yandex)."""

    def __init__(
        self,
        token_service: TokenService,
        logger: logging.Logger
    ):
        self.token_service = token_service
        self.logger = logger

    async def oauth_login_or_register(
        self,
        uow: UnitOfWork,
        provider: str,
        provider_id: str,
        email: str,
        full_name: str | None,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> AuthResult:
        """Login or register user via OAuth."""
        user = await uow.users.get_by_email(email)

        if not user:
            user = await uow.users.create_user(
                email=email,
                password_hash=None,
                full_name=full_name,
                role=UserRole.USER.value,
                oauth_provider=provider,
                oauth_provider_id=provider_id,
            )

            await uow.security_events.log_event(
                event_type="oauth_register",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"provider": provider},
            )

            self.logger.info(f"OAuth user registered: {email} ({provider})")
        else:
            if not user.is_active:
                raise AuthenticationError("User account is inactive")

            if not user.is_email_verified:
                user.is_email_verified = True

            await uow.users.update_last_login(user.id)

            await uow.security_events.log_event(
                event_type="oauth_login",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"provider": provider},
            )

            self.logger.info(f"OAuth user logged in: {email} ({provider})")

        tokens = self.token_service.generate_tokens(user)

        return AuthResult(
            user=user,
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
        )

    async def yandex_token_login(
        self,
        uow: UnitOfWork,
        yandex_token: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> AuthResult:
        """Login or register user via Yandex implicit flow (YaAuthSuggest SDK)."""
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "https://login.yandex.ru/info",
                params={"format": "json"},
                headers={"Authorization": f"OAuth {yandex_token}"},
                timeout=10,
            )

        if r.status_code != 200:
            raise AuthenticationError("Invalid Yandex token")

        userinfo = r.json()
        email: str = userinfo.get("default_email") or userinfo["emails"][0]

        return await self.oauth_login_or_register(
            uow=uow,
            provider="yandex",
            provider_id=str(userinfo["id"]),
            email=email,
            full_name=userinfo.get("display_name"),
            ip_address=ip_address,
            user_agent=user_agent,
        )
