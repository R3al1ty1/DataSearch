from collections.abc import AsyncIterator
from datetime import UTC, datetime
import logging
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lib.auth.dependencies import get_current_active_user, get_current_user, get_uow
from lib.auth.models import User
from lib.auth.services.auth_service import AuthService
from lib.core.constants import UserRole
from lib.core.container import container
from lib.core.exceptions import register_exception_handlers
from lib.router import api_router


class DummyDatasets:
    async def get_by_id(self, dataset_id):
        return None


class DummyUsers:
    def __init__(self):
        self.by_id = {}

    async def get_by_id(self, user_id):
        return self.by_id.get(user_id)

    async def create(self, user):
        now = datetime.now(UTC)
        user.created_at = now
        user.updated_at = now
        self.by_id[user.id] = user
        return user


class DummyUnitOfWork:
    datasets = DummyDatasets()

    def __init__(self):
        self.users = DummyUsers()
        self.commits = 0

    async def commit(self):
        self.commits += 1


async def override_uow() -> AsyncIterator[DummyUnitOfWork]:
    yield DummyUnitOfWork()


def make_user(is_active: bool = True) -> User:
    now = datetime.now(UTC)
    return User(
        id=uuid4(),
        email="user@example.com",
        full_name=None,
        role=UserRole.USER.value,
        is_active=is_active,
        is_email_verified=True,
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    register_exception_handlers(app, logging.getLogger("test"))
    app.include_router(api_router, prefix="/api")
    app.dependency_overrides[get_uow] = override_uow
    test_client = TestClient(app)

    yield test_client

    test_client.close()
    app.dependency_overrides.clear()


def test_auth_me_without_authorization_header_returns_missing_auth_header(client):
    response = client.get("/api/auth/me")

    assert response.status_code == 401
    assert response.json()["error_code"] == "MISSING_AUTH_HEADER"


def test_auth_refresh_without_cookie_returns_missing_refresh_token(client):
    response = client.post("/api/auth/refresh")

    assert response.status_code == 401
    assert response.json()["error_code"] == "MISSING_REFRESH_TOKEN"


def test_search_click_with_missing_dataset_returns_dataset_not_found(client):
    client.app.dependency_overrides[get_current_active_user] = lambda: make_user()
    dataset_id = uuid4()

    response = client.post(
        "/api/search/click",
        json={
            "dataset_id": str(dataset_id),
            "position": 0,
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "error_code": "DATASET_NOT_FOUND",
        "message": "Dataset not found",
        "details": {"resource": "Dataset", "identifier": str(dataset_id)},
    }


def test_invalid_request_body_returns_validation_error(client):
    client.app.dependency_overrides[get_current_active_user] = lambda: make_user()

    response = client.post("/api/search/click", json={})

    assert response.status_code == 422
    assert response.json()["error_code"] == "VALIDATION_ERROR"


def test_protected_endpoint_with_inactive_user_returns_account_inactive(client):
    client.app.dependency_overrides[get_current_user] = lambda: make_user(is_active=False)

    response = client.get("/api/auth/me")

    assert response.status_code == 403
    assert response.json()["error_code"] == "ACCOUNT_INACTIVE"


def test_gateway_auth_creates_user_from_forwarded_identity(client, monkeypatch):
    monkeypatch.setitem(
        container.__dict__,
        "auth_service",
        AuthService(
            token_service=None,
            rate_limit_service=None,
            settings=None,
            logger=logging.getLogger("test"),
        ),
    )
    monkeypatch.setitem(
        container.__dict__,
        "settings",
        type("Settings", (), {"DATASEARCH_SERVICE_TOKEN": "service-secret"})(),
    )

    response = client.get(
        "/api/auth/me",
        headers={
            "x-service-token": "service-secret",
            "x-user-id": str(uuid4()),
            "x-user-role": UserRole.ADMIN.value,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["email"].endswith("@gateway.local")
    assert body["role"] == UserRole.ADMIN.value
    assert body["is_active"] is True


def test_gateway_auth_rejects_wrong_service_token(client, monkeypatch):
    monkeypatch.setitem(
        container.__dict__,
        "settings",
        type("Settings", (), {"DATASEARCH_SERVICE_TOKEN": "service-secret"})(),
    )

    response = client.get(
        "/api/auth/me",
        headers={
            "x-service-token": "wrong",
            "x-user-id": str(uuid4()),
        },
    )

    assert response.status_code == 401
    assert response.json()["error_code"] == "MISSING_AUTH_HEADER"


def test_openapi_documents_common_error_responses(client):
    response = client.get("/openapi.json")

    assert response.status_code == 200
    operation = response.json()["paths"]["/api/search/click"]["post"]
    for status_code in ("401", "403", "404", "422", "500"):
        assert operation["responses"][status_code]["content"]["application/json"][
            "schema"
        ] == {"$ref": "#/components/schemas/ErrorResponse"}
