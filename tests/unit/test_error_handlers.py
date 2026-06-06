import logging

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from lib.auth.exceptions import MissingAuthHeader
from lib.core.exceptions import register_exception_handlers
from lib.services.datasets.exceptions import DatasetNotFound


class Payload(BaseModel):
    name: str


def build_app() -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app, logging.getLogger("test"))

    @app.get("/auth-required")
    async def auth_required():
        raise MissingAuthHeader()

    @app.get("/datasets/{dataset_id}")
    async def dataset(dataset_id: str):
        raise DatasetNotFound(dataset_id)

    @app.post("/payload")
    async def payload(body: Payload):
        return body

    @app.get("/crash")
    async def crash():
        raise RuntimeError("raw failure")

    return app


def test_domain_error_uses_standard_body():
    client = TestClient(build_app())

    response = client.get("/datasets/missing")

    assert response.status_code == 404
    assert response.json() == {
        "error_code": "DATASET_NOT_FOUND",
        "message": "Dataset not found",
        "details": {"resource": "Dataset", "identifier": "missing"},
    }


def test_auth_error_uses_standard_body_and_headers():
    client = TestClient(build_app())

    response = client.get("/auth-required")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json() == {
        "error_code": "MISSING_AUTH_HEADER",
        "message": "Authorization header is required",
        "details": None,
    }


def test_validation_error_uses_standard_body():
    client = TestClient(build_app())

    response = client.post("/payload", json={})

    assert response.status_code == 422
    body = response.json()
    assert body["error_code"] == "VALIDATION_ERROR"
    assert body["message"] == "Request validation failed"
    assert body["details"]["fields"] == [
        {"field": "body.name", "message": "Field required"}
    ]


def test_domain_error_log_includes_observability_fields(caplog):
    client = TestClient(build_app())

    response = client.get(
        "/datasets/missing",
        headers={"X-Request-ID": "request-123"},
    )

    assert response.status_code == 404
    assert response.headers["x-request-id"] == "request-123"
    assert "error_code=DATASET_NOT_FOUND" in caplog.text
    assert "status_code=404" in caplog.text
    assert "method=GET" in caplog.text
    assert "path=/datasets/missing" in caplog.text
    assert "request_id=request-123" in caplog.text


def test_validation_error_log_includes_safe_observability_fields(caplog):
    client = TestClient(build_app())

    response = client.post(
        "/payload",
        json={},
        headers={"X-Correlation-ID": "correlation-456"},
    )

    assert response.status_code == 422
    assert response.headers["x-request-id"] == "correlation-456"
    assert "error_code=VALIDATION_ERROR" in caplog.text
    assert "status_code=422" in caplog.text
    assert "method=POST" in caplog.text
    assert "path=/payload" in caplog.text
    assert "request_id=correlation-456" in caplog.text


def test_unhandled_error_logs_safe_internal_error(caplog):
    client = TestClient(build_app(), raise_server_exceptions=False)

    response = client.get("/crash", headers={"X-Request-ID": "request-789"})

    assert response.status_code == 500
    assert response.json() == {
        "error_code": "INTERNAL_ERROR",
        "message": "An unexpected error occurred",
        "details": None,
    }
    assert "error_code=INTERNAL_ERROR" in caplog.text
    assert "status_code=500" in caplog.text
    assert "path=/crash" in caplog.text
    assert "request_id=request-789" in caplog.text
    assert "raw failure" not in response.text
