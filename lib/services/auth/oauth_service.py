import logging
import secrets
from dataclasses import dataclass

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.config import Settings
from lib.core.redis_auth import RedisAuthManager
from lib.core.constants import AuthConstants, UserRole
from lib.core.exceptions import AuthenticationError
from lib.repositories.user import UserRepository
from lib.repositories.security_event import SecurityEventRepository
from lib.services.auth.token_service import TokenService
from lib.services.auth.auth_service import AuthResult


@dataclass
class OAuthUserInfo:
    """OAuth user information."""
    provider_id: str
    email: str
    full_name: str | None


class OAuthService:
    """Service for OAuth authentication (Google, Yandex)."""

    def __init__(
        self,
        user_repo: UserRepository,
        security_event_repo: SecurityEventRepository,
        token_service: TokenService,
        redis_manager: RedisAuthManager,
        settings: Settings,
        logger: logging.Logger
    ):
        self.user_repo = user_repo
        self.security_event_repo = security_event_repo
        self.token_service = token_service
        self.redis_manager = redis_manager
        self.settings = settings
        self.logger = logger

    async def generate_oauth_state(self) -> str:
        """Generate and store OAuth state for CSRF protection."""
        state = secrets.token_urlsafe(32)
        key = f"oauth:state:{state}"

        async for redis in self.redis_manager.get_session():
            await redis.set(
                key, "1", ex=AuthConstants.OAUTH_STATE_EXPIRE_SECONDS
            )

        return state

    async def verify_oauth_state(self, state: str) -> bool:
        """Verify OAuth state and consume it."""
        key = f"oauth:state:{state}"

        async for redis in self.redis_manager.get_session():
            exists = await redis.exists(key) > 0
            if exists:
                await redis.delete(key)
            return exists

        return False

    def get_google_auth_url(self, state: str) -> str:
        """Get Google OAuth authorization URL."""
        if not self.settings.GOOGLE_CLIENT_ID:
            raise AuthenticationError("Google OAuth not configured")

        params = {
            "client_id": self.settings.GOOGLE_CLIENT_ID,
            "redirect_uri": self.settings.GOOGLE_REDIRECT_URI,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state
        }

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"https://accounts.google.com/o/oauth2/v2/auth?{query}"

    async def exchange_google_code(self, code: str) -> OAuthUserInfo:
        """Exchange Google authorization code for user info."""
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code,
                    "client_id": self.settings.GOOGLE_CLIENT_ID,
                    "client_secret": self.settings.GOOGLE_CLIENT_SECRET,
                    "redirect_uri": self.settings.GOOGLE_REDIRECT_URI,
                    "grant_type": "authorization_code"
                }
            )

            if token_response.status_code != 200:
                raise AuthenticationError("Failed to exchange Google code")

            tokens = token_response.json()
            access_token = tokens["access_token"]

            user_info_response = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {access_token}"}
            )

            if user_info_response.status_code != 200:
                raise AuthenticationError("Failed to get Google user info")

            user_info = user_info_response.json()

            return OAuthUserInfo(
                provider_id=user_info["id"],
                email=user_info["email"],
                full_name=user_info.get("name")
            )

    def get_yandex_auth_url(self, state: str) -> str:
        """Get Yandex OAuth authorization URL."""
        if not self.settings.YANDEX_CLIENT_ID:
            raise AuthenticationError("Yandex OAuth not configured")

        params = {
            "client_id": self.settings.YANDEX_CLIENT_ID,
            "redirect_uri": self.settings.YANDEX_REDIRECT_URI,
            "response_type": "code",
            "state": state
        }

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"https://oauth.yandex.ru/authorize?{query}"

    async def exchange_yandex_code(self, code: str) -> OAuthUserInfo:
        """Exchange Yandex authorization code for user info."""
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://oauth.yandex.ru/token",
                data={
                    "code": code,
                    "client_id": self.settings.YANDEX_CLIENT_ID,
                    "client_secret": self.settings.YANDEX_CLIENT_SECRET,
                    "grant_type": "authorization_code"
                }
            )

            if token_response.status_code != 200:
                raise AuthenticationError("Failed to exchange Yandex code")

            tokens = token_response.json()
            access_token = tokens["access_token"]

            user_info_response = await client.get(
                "https://login.yandex.ru/info",
                headers={"Authorization": f"Bearer {access_token}"}
            )

            if user_info_response.status_code != 200:
                raise AuthenticationError("Failed to get Yandex user info")

            user_info = user_info_response.json()

            return OAuthUserInfo(
                provider_id=user_info["id"],
                email=user_info["default_email"],
                full_name=user_info.get("display_name")
            )

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
            user = await self.user_repo.get_by_email(session, email)

            if user:
                raise AuthenticationError(
                    f"Email {email} already registered with password. "
                    "Please login normally."
                )

            user = await self.user_repo.create_user(
                session=session,
                email=email,
                password_hash=None,
                full_name=full_name,
                role=UserRole.USER.value,
                oauth_provider=provider,
                oauth_provider_id=provider_id
            )

            await session.commit()

            await self.security_event_repo.log_event(
                session=session,
                event_type="oauth_register",
                user_id=user.id,
                ip_address=ip_address,
                user_agent=user_agent,
                details={"provider": provider}
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
                details={"provider": provider}
            )
            await session.commit()

            self.logger.info(f"OAuth user logged in: {email} ({provider})")

        tokens = self.token_service.generate_tokens(user)

        return AuthResult(
            user=user,
            access_token=tokens.access_token,
            refresh_token=tokens.refresh_token,
            token_type=tokens.token_type
        )
