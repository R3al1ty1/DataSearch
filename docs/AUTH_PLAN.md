# Authentication & Authorization Implementation Plan

## Context

The DataSearch project currently has no authentication system - all API endpoints are publicly accessible. This poses security risks and prevents user tracking, personalization, and access control. This plan implements a production-ready JWT-based authentication system with:

- User registration and login with secure password hashing
- JWT access tokens (30min) and refresh tokens (7 days) with blacklist support
- Role-based authorization (USER, ADMIN roles)
- Rate limiting and brute-force protection
- OAuth2 integration for Google and Yandex ID
- Security event logging for audit trails
- Protection of existing endpoints (search, tracking, admin tasks)

**User Requirements:**
- ✅ No dummyLogin endpoint (removed from previous implementation)
- ✅ Use Enums/constants instead of hardcoded values
- ✅ Refresh token flow with Redis blacklist
- ✅ Brute-force protection via rate limiting
- ✅ Password validation with complexity rules
- ✅ Security event logging using existing logger pattern
- ✅ OAuth support for Google and Yandex
- ✅ Protect all non-public endpoints

**Architectural Alignment:**
The implementation follows existing DataSearch patterns:
- SQLAlchemy 2.0 async models with Base, UUIDMixin, TimestampMixin
- Repository pattern (BaseRepository → specialized repos)
- Service layer with dependency injection via AppContainer
- Pydantic schemas with modern type hints
- FastAPI handlers with container.db.get_session and container.logger_manager.get_logger
- Redis database separation (DB 0 for Celery, DB 1 for auth)

---

## Implementation Steps

### 1. Foundation Layer - Constants & Configuration

**File: `lib/core/constants.py`**
Add:
- `UserRole(str, Enum)` with USER and ADMIN values
- `AuthConstants` class with JWT settings, password rules, rate limits, Redis prefixes

**File: `lib/core/config.py`**
Add to Settings:
- `JWT_SECRET_KEY: str` (required in .env)
- `JWT_ALGORITHM`, `ACCESS_TOKEN_EXPIRE_MINUTES`, `REFRESH_TOKEN_EXPIRE_DAYS`
- Google OAuth: `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REDIRECT_URI`
- Yandex OAuth: `YANDEX_CLIENT_ID`, `YANDEX_CLIENT_SECRET`, `YANDEX_REDIRECT_URI`
- `REDIS_AUTH_URL` property returning `redis://host:port/1`

**Update: `.env.example`**
Add JWT_SECRET_KEY and OAuth credentials (optional)

### 2. Database Models

**File: `lib/models/user.py` (NEW)**
```python
class User(Base, UUIDMixin, TimestampMixin):
    email: unique, indexed
    password_hash: nullable (for OAuth users)
    full_name: nullable
    role: enum (user, admin), indexed
    is_active, is_email_verified: booleans
    last_login_at: nullable datetime
    oauth_provider, oauth_provider_id: nullable strings
    __table_args__: unique index on (oauth_provider, oauth_provider_id)
```

**File: `lib/models/security_event.py` (NEW)**
```python
class SecurityEvent(Base, UUIDMixin):
    user_id: nullable UUID, indexed
    event_type: string, indexed
    ip_address, user_agent: nullable strings
    details: JSONB
    created_at: datetime, indexed
```

**Migration:** Create `user_role_enum`, `users` table, `security_events` table with all indexes

### 3. Exceptions

**File: `lib/core/exceptions.py`**
Add authentication exceptions:
- `AuthenticationError` (base)
- `InvalidCredentials`, `TokenExpired`, `TokenInvalid`, `TokenBlacklisted`
- `UserNotFound`, `UserAlreadyExists`, `PasswordValidationError`
- `RateLimitExceeded`, `InsufficientPermissions`

### 4. Redis Auth Manager

**File: `lib/core/redis_auth.py` (NEW)**
```python
class RedisAuthManager:
    __init__(redis_url, logger)
    async connect(), async close()
    redis property (raises if not connected)
    async set(key, value, ex), async get(key), async delete(key)
    async exists(key), async incr(key), async expire(key, seconds), async ttl(key)
```

### 5. Core Auth Utilities

**File: `lib/core/auth/password.py` (NEW)**
- `hash_password(password)` using passlib bcrypt
- `verify_password(plain, hashed)`
- `validate_password(password)` raises PasswordValidationError if invalid
  - Check length (8-128), uppercase, lowercase, digit, special char (configurable)

**File: `lib/core/auth/jwt.py` (NEW)**
- `create_access_token(user_id, email, role, settings)` returns JWT
- `create_refresh_token(user_id, settings)` returns JWT
- `decode_token(token, settings)` raises TokenExpired or TokenInvalid
- `get_token_jti(token, settings)` for blacklist key
- `get_token_expiry(token, settings)` returns seconds until expiry

### 6. Repository Layer

**File: `lib/repositories/user.py` (NEW)**
```python
class UserRepository(BaseRepository[User]):
    async get_by_email(session, email)
    async get_by_oauth(session, provider, provider_id)
    async email_exists(session, email)
    async update_last_login(session, user_id)
    async create_user(session, email, password_hash, full_name, role, oauth_provider, oauth_provider_id)
```

**File: `lib/repositories/security_event.py` (NEW)**
```python
class SecurityEventRepository(BaseRepository[SecurityEvent]):
    async log_event(session, event_type, user_id, ip_address, user_agent, details)
```

Export both in `lib/repositories/__init__.py`

### 7. Service Layer

**File: `lib/services/auth/token_service.py` (NEW)**
```python
class TokenService:
    __init__(redis, settings, logger)
    generate_tokens(user) -> dict (access_token, refresh_token, token_type)
    async blacklist_token(token)
    async is_token_blacklisted(token) -> bool
```

**File: `lib/services/auth/rate_limit_service.py` (NEW)**
```python
class RateLimitService:
    __init__(redis, logger)
    async check_rate_limit(identifier) raises RateLimitExceeded
    async increment_attempt(identifier) -> int
    async reset_attempts(identifier)
```

**File: `lib/services/auth/auth_service.py` (NEW)**
```python
class AuthService:
    __init__(user_repo, security_event_repo, token_service, rate_limit_service, settings, logger)
    async register(session, email, password, full_name, ip, user_agent) -> dict
    async login(session, email, password, ip, user_agent) -> dict
    async refresh_token(session, refresh_token) -> dict
    async logout(session, access_token, user_id, ip, user_agent)
    async get_current_user(session, token) -> User
```

**File: `lib/services/auth/oauth_service.py` (NEW)**
```python
class OAuthService:
    __init__(user_repo, security_event_repo, token_service, redis, settings, logger)
    async generate_oauth_state() -> str
    async verify_oauth_state(state) -> bool
    get_google_auth_url(state) -> str
    async exchange_google_code(code) -> dict
    get_yandex_auth_url(state) -> str
    async exchange_yandex_code(code) -> dict
    async oauth_login_or_register(session, provider, provider_id, email, full_name, ip, user_agent) -> dict
```

### 8. Pydantic Schemas

**File: `lib/schemas/auth.py` (NEW)**
```python
RegisterRequest(email, password, full_name)
LoginRequest(email, password)
RefreshTokenRequest(refresh_token)
UserResponse(id, email, full_name, role, is_active, is_email_verified, last_login_at, created_at)
TokenResponse(access_token, refresh_token, token_type)
AuthResponse(user, access_token, refresh_token, token_type)
OAuthUrlResponse(auth_url, state)
```

### 9. FastAPI Dependencies

**File: `lib/api/dependencies/auth.py` (NEW)**
```python
security = HTTPBearer()

async def get_current_user(request, credentials, db) -> User
    - Extracts token from Authorization header
    - Calls container.auth_service.get_current_user()
    - Returns User or raises 401

async def get_current_active_user(current_user) -> User
    - Checks is_active
    - Returns User or raises 403

def require_role(required_role: UserRole)
    - Returns dependency function that checks user role
    - Admins bypass all role checks
    - Raises 403 if insufficient permissions

def get_ip_address(request) -> str | None
def get_user_agent(request) -> str | None
```

### 10. API Handlers

**File: `lib/api/handlers/auth.py` (NEW)**
```python
router = APIRouter(tags=["Authentication"])

POST /auth/register -> AuthResponse (201)
POST /auth/login -> AuthResponse
POST /auth/refresh -> TokenResponse
POST /auth/logout -> 204 (requires auth)
GET /auth/me -> UserResponse (requires auth)
```

**File: `lib/api/handlers/oauth.py` (NEW)**
```python
router = APIRouter(tags=["OAuth"])

GET /auth/oauth/google -> OAuthUrlResponse
GET /auth/oauth/google/callback?code=...&state=... -> AuthResponse
GET /auth/oauth/yandex -> OAuthUrlResponse
GET /auth/oauth/yandex/callback?code=...&state=... -> AuthResponse
```

### 11. Container Registration

**File: `lib/core/container.py`**
Add @cached_property for:
- `redis_auth` -> RedisAuthManager(REDIS_AUTH_URL, logger)
- `user_repo` -> UserRepository()
- `security_event_repo` -> SecurityEventRepository()
- `token_service` -> TokenService(redis_auth, settings, logger)
- `rate_limit_service` -> RateLimitService(redis_auth, logger)
- `auth_service` -> AuthService(user_repo, security_event_repo, token_service, rate_limit_service, settings, logger)
- `oauth_service` -> OAuthService(user_repo, security_event_repo, token_service, redis_auth, settings, logger)

**File: `lib/main.py`**
Update lifespan:
- Add `await container.redis_auth.connect()` after DB init
- Add `await container.redis_auth.close()` on shutdown

### 12. Protect Existing Endpoints

**File: `lib/api/handlers/search.py`**
```python
async def search_datasets(
    body: SearchRequest,
    current_user: Annotated[User, Depends(get_current_active_user)],  # ADD
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
)
```

**File: `lib/api/handlers/tracking.py`**
```python
async def visit_dataset(
    dataset_id: UUID,
    current_user: Annotated[User, Depends(get_current_active_user)],  # ADD
    db: AsyncSession = Depends(container.db.get_session),
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
)
```

**File: `lib/api/handlers/system.py`**
```python
async def trigger_embedding_generation(
    request: EmbeddingTaskRequest = EmbeddingTaskRequest(),
    current_user: Annotated[User, Depends(require_role(UserRole.ADMIN))],  # ADD ADMIN ONLY
    logger: logging.Logger = Depends(container.logger_manager.get_logger)
)
```

**File: `lib/api/handlers/router.py`**
```python
from lib.api.handlers import system, search, tracking, auth, oauth

api_router.include_router(auth.router)    # ADD
api_router.include_router(oauth.router)   # ADD
```

### 13. Dependencies

**File: `pyproject.toml`**
Add dependencies:
```toml
"passlib[bcrypt]>=1.7.4",
"python-jose[cryptography]>=3.3.0",
"python-multipart>=0.0.9",
"email-validator>=2.1.0"
```

Run: `uv sync`

---

## Critical Files to Modify/Create

**New Files (18):**
1. `lib/models/user.py` - User model
2. `lib/models/security_event.py` - Security audit log
3. `lib/core/redis_auth.py` - Redis manager for auth
4. `lib/core/auth/password.py` - Password utilities
5. `lib/core/auth/jwt.py` - JWT utilities
6. `lib/repositories/user.py` - User repository
7. `lib/repositories/security_event.py` - Security event repository
8. `lib/services/auth/token_service.py` - Token management
9. `lib/services/auth/rate_limit_service.py` - Rate limiting
10. `lib/services/auth/auth_service.py` - Auth business logic
11. `lib/services/auth/oauth_service.py` - OAuth flows
12. `lib/schemas/auth.py` - Auth Pydantic models
13. `lib/api/dependencies/auth.py` - Auth dependencies
14. `lib/api/handlers/auth.py` - Auth endpoints
15. `lib/api/handlers/oauth.py` - OAuth endpoints
16. Database migration file - Create tables and enums
17. `.env.example` - Add JWT and OAuth settings
18. `lib/api/dependencies/__init__.py` - Package init

**Existing Files to Modify (8):**
1. `lib/core/constants.py` - Add UserRole enum and AuthConstants
2. `lib/core/config.py` - Add JWT and OAuth settings
3. `lib/core/exceptions.py` - Add auth exceptions
4. `lib/core/container.py` - Register auth services
5. `lib/main.py` - Initialize redis_auth in lifespan
6. `lib/api/handlers/search.py` - Add auth dependency
7. `lib/api/handlers/tracking.py` - Add auth dependency
8. `lib/api/handlers/system.py` - Add admin role requirement
9. `lib/api/handlers/router.py` - Include auth and oauth routers
10. `lib/repositories/__init__.py` - Export new repositories
11. `lib/models/__init__.py` - Export User model
12. `pyproject.toml` - Add dependencies

---

## Reusable Existing Code

- **BaseRepository** pattern from `lib/repositories/base.py` - Reuse for UserRepository and SecurityEventRepository
- **UUIDMixin, TimestampMixin** from `lib/models/base.py` - Apply to User and SecurityEvent models
- **Container pattern** from `lib/core/container.py` - Register all auth services with @cached_property
- **Logger injection** pattern from existing handlers - Use `Depends(container.logger_manager.get_logger)`
- **Database session** pattern - Use `Depends(container.db.get_session)`
- **Settings** pattern from `lib/core/config.py` - Extend with JWT and OAuth settings
- **Exception hierarchy** from `lib/core/exceptions.py` - Extend with auth exceptions
- **Pydantic schemas** pattern from `lib/schemas/` - Follow same structure for auth schemas

---

## Implementation Order

**Phase 1: Foundation (No external dependencies)**
1. Constants & Config (step 1)
2. Exceptions (step 3)
3. Database Models (step 2)

**Phase 2: Infrastructure (Depends on Phase 1)**
4. Redis Auth Manager (step 4)
5. Password utilities (step 5)
6. JWT utilities (step 5)

**Phase 3: Data Layer (Depends on Phase 1-2)**
7. Repositories (step 6)
8. Database migration

**Phase 4: Services (Depends on Phase 1-3)**
9. Token Service (step 7)
10. Rate Limit Service (step 7)
11. Auth Service (step 7)
12. OAuth Service (step 7)

**Phase 5: API (Depends on Phase 1-4)**
13. Schemas (step 8)
14. Dependencies (step 9)
15. Handlers (step 10)

**Phase 6: Integration (Depends on all phases)**
16. Container registration (step 11)
17. Lifespan update (step 11)
18. Endpoint protection (step 12)
19. Router registration (step 12)

**Phase 7: Dependencies & Testing**
20. Install packages (step 13)
21. Run migrations
22. Test endpoints

---

## Verification & Testing

### 1. Database Verification
```bash
# Check tables created
psql -U user -d datasearch_db -c "\dt"
# Should show: users, security_events tables

# Check enum created
psql -U user -d datasearch_db -c "\dT user_role_enum"
# Should show: user, admin values
```

### 2. Redis Verification
```bash
# Connect to Redis auth database
redis-cli -n 1 KEYS "*"
# Should be empty initially

# After login attempt, check rate limit
redis-cli -n 1 KEYS "ratelimit:*"
```

### 3. API Testing (with curl or Swagger /docs)

**Register:**
```bash
curl -X POST http://localhost:8000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Test1234","full_name":"Test User"}'
# Expected: 201 with user data and tokens
```

**Login:**
```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Test1234"}'
# Expected: 200 with user data and tokens
```

**Access Protected Endpoint:**
```bash
# Without token
curl -X POST http://localhost:8000/api/search \
  -H "Content-Type: application/json" \
  -d '{"query":"machine learning","limit":10}'
# Expected: 401 Unauthorized

# With token
curl -X POST http://localhost:8000/api/search \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <access_token>" \
  -d '{"query":"machine learning","limit":10}'
# Expected: 200 with search results
```

**Rate Limiting:**
```bash
# Try login 6 times with wrong password
for i in {1..6}; do
  curl -X POST http://localhost:8000/api/auth/login \
    -H "Content-Type: application/json" \
    -d '{"email":"test@example.com","password":"wrong"}'
done
# Expected: First 5 attempts return 401, 6th returns 429 (rate limited)
```

**Token Refresh:**
```bash
curl -X POST http://localhost:8000/api/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<refresh_token>"}'
# Expected: 200 with new access_token and refresh_token
```

**Logout:**
```bash
curl -X POST http://localhost:8000/api/auth/logout \
  -H "Authorization: Bearer <access_token>"
# Expected: 204 No Content

# Try using same token after logout
curl -X GET http://localhost:8000/api/auth/me \
  -H "Authorization: Bearer <access_token>"
# Expected: 401 (token blacklisted)
```

**Admin Endpoint:**
```bash
# As regular user
curl -X POST http://localhost:8000/api/tasks/generate-embeddings \
  -H "Authorization: Bearer <user_token>"
# Expected: 403 Forbidden

# As admin user (create admin via SQL first)
curl -X POST http://localhost:8000/api/tasks/generate-embeddings \
  -H "Authorization: Bearer <admin_token>"
# Expected: 200 with task trigger response
```

**OAuth Flow:**
```bash
# Get Google OAuth URL
curl http://localhost:8000/api/auth/oauth/google
# Expected: {"auth_url": "https://accounts.google.com/...", "state": "..."}
# Visit auth_url in browser, approve, get redirected to callback
# Callback should return user data and tokens
```

### 4. Security Events Verification
```sql
-- Check security events logged
SELECT event_type, user_id, ip_address, created_at, details
FROM security_events
ORDER BY created_at DESC
LIMIT 10;

-- Should show: user_registered, login_success, login_failed, logout, oauth_login, etc.
```

### 5. End-to-End Flow
1. Register new user → verify user in DB, security event logged
2. Login → verify tokens returned, last_login_at updated, security event logged
3. Access protected endpoint → verify access granted
4. Logout → verify token blacklisted in Redis
5. Try accessing with logged-out token → verify 401 error
6. Refresh token → verify new tokens issued
7. Wrong password 5+ times → verify rate limiting kicks in
8. OAuth flow (if configured) → verify user created/logged in

---

## Security Notes

1. **JWT_SECRET_KEY**: Must be strong (32+ characters), random, and kept secret. Generate with:
   ```bash
   openssl rand -hex 32
   ```

2. **Production Checklist:**
   - [ ] Use HTTPS (configure reverse proxy like nginx)
   - [ ] Set strong JWT_SECRET_KEY
   - [ ] Configure CORS properly (remove allow_origins=["*"])
   - [ ] Set up Redis persistence: `redis-server --appendonly yes`
   - [ ] Rotate JWT_SECRET_KEY periodically
   - [ ] Monitor security_events table for suspicious activity
   - [ ] Set up alerts for rate limit violations
   - [ ] Use environment-specific .env files (dev/staging/prod)
   - [ ] Configure OAuth redirect URIs for production domain
   - [ ] Set DEBUG=False in production

3. **Password Security:**
   - Bcrypt with default rounds (12) - secure and performant
   - Passwords hashed before storage
   - Plain passwords never logged or stored

4. **Token Security:**
   - Short-lived access tokens (30 min) limit exposure
   - Refresh tokens rotated on each use (if implemented)
   - Blacklist ensures immediate revocation on logout
   - Redis TTL auto-expires blacklist entries

5. **Rate Limiting:**
   - Protects against brute-force attacks
   - 5 attempts per 5 minutes per email
   - Can be adjusted in AuthConstants

---

## Estimated Implementation Time

- **Phase 1-2 (Foundation + Infrastructure)**: 3-4 hours
- **Phase 3 (Data Layer)**: 2-3 hours
- **Phase 4 (Services)**: 4-5 hours
- **Phase 5 (API)**: 2-3 hours
- **Phase 6 (Integration)**: 1-2 hours
- **Phase 7 (Testing)**: 2-3 hours

**Total**: 14-20 hours (2-3 days of focused work)

OAuth setup adds ~2 hours for Google/Yandex configuration and testing.
