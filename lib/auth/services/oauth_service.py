import logging

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from lib.auth.repository import SecurityEventRepository, UserRepository
from lib.auth.services.auth_service import AuthResult
from lib.auth.services.token_service import TokenService
from lib.core.constants import UserRole
from lib.core.exceptions import AuthenticationError


class OAuthService:
    """Service for OAuth authentication (Google, Yandex)."""

    def __init__(
        self,
        user_repo: UserRepository,
        security_event_repo: SecurityEventRepository,
        token_service: TokenService,
        logger: logging.Logger
    ):
        self.user_repo = user_repo
        self.security_event_repo = security_event_repo
        self.token_service = token_service
        self.logger = logger

    async def oauth_login_or_register(
        self,
        session: AsyncSession,
        provider: str,
        provider_id: str,
        email: str,
        full_name: str | None,
        ip_address: str | None = None,
        user_agent: str | None = None
    ) -> AuthResult:
        """Login or register user via OAuth."""
        user = await self.user_repo.get_by_oauth(session, provider, provider_id)

        if not user:
            existing = await self.user_repo.get_by_email(session, email)
            if existing:
                raise AuthenticationError(
                    f"Email {email} is already registered with a password. Please login normally."
                )

            user = await self.user_repo.create_user(
                session=session,
                email=email,
                password_hash=None,
                full_name=full_name,
                role=UserRole.USER.value,
                oauth_provider=provider,
                oauth_provider_id=provider_id,
            )
            await session.commit()

            await self.security_event_repo.log_event(
                session=session,
                event_type="oauth_register",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"provider": provider},
            )
            await session.commit()

            self.logger.info(f"OAuth user registered: {email} ({provider})")
        else:
            await self.user_repo.update_last_login(session, user.id)

            await self.security_event_repo.log_event(
                session=session,
                event_type="oauth_login",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"provider": provider},
            )
            await session.commit()

            self.logger.info(f"OAuth user logged in: {email} ({provider})")

        tokens = self.token_service.generate_tokens(user)

        return AuthResult(
            user=user,
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
        )

    async def yandex_token_login(
        self,
        session: AsyncSession,
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
            session=session,
            provider="yandex",
            provider_id=str(userinfo["id"]),
            email=email,
            full_name=userinfo.get("display_name"),
            ip_address=ip_address,
            user_agent=user_agent,
        )
