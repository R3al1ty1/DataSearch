# Exception Best Practice TODO

This project now has a structured exception foundation:

- stable `ErrorCode` values in `lib/core/error_codes.py`
- centralized FastAPI handlers in `lib/core/exceptions.py`
- domain exception modules for auth, datasets, enrichment, ML, and system tasks
- documented API error shape in `docs/API_ERRORS.md`

The remaining work below would take the exception system from good to best-practice level.

## 1. Publish The Contract In OpenAPI

The JSON contract exists, but route metadata should expose it in generated docs.

- Add shared `responses` definitions for common errors.
- Attach common responses to routers/endpoints: `401`, `403`, `404`, `422`, `500`.
- Reuse `lib.system.schemas.ErrorResponse` for docs.
- Keep `/docs` aligned with `docs/API_ERRORS.md`.

## 2. Reduce Broad `except Exception`

Some broad catches are acceptable at boundaries, but several can be more precise.

- In ingestion scripts, catch `DataSearchError` separately and log `error_code`, `message`, and `details`.
- In parser/client internals, prefer concrete types such as Pydantic `ValidationError`, `httpx.HTTPError`, and Kaggle SDK errors when available.
- Replace `except Exception: pass` in auth logout token blacklisting with explicit logging or narrower exception handling.
- Keep programming invariant errors as plain `RuntimeError` where appropriate.

## 3. Standardize Task And Worker Error Serialization

API errors are standardized, but Celery tasks should also expose structured domain failures.

- Add a helper such as `serialize_error(exc)`.
- For `DataSearchError`, return/log `error_code`, `message`, and `details`.
- For unexpected exceptions, return/log `INTERNAL_ERROR` with a safe message.
- Avoid leaking raw stack traces or secrets into task results.

## 4. Improve Enrichment Error Logs

Enrichment logs should use stable codes, not only class names or raw strings.

- Store `error_type=error.error_code.value` for `DataSearchError`.
- Keep class name only as optional diagnostic context.
- Include structured details where useful: `source`, `stage`, `retry_after`, external status code.
- Keep low-level `httpx` errors inside clients for retry behavior, but convert them at processor boundaries.

## 5. Add Repository And Database Boundary Errors

Do not convert every database issue, but business-relevant persistence failures should have domain meaning.

- Duplicate dataset/source identity -> dataset conflict error.
- Missing related record during click/search-log operations -> domain not-found error where applicable.
- Database unavailable on API paths -> safe service/internal error response.
- Preserve raw invariant failures such as "Database not initialized" and "UnitOfWork session is not initialized".

## 6. Add Endpoint-Level Tests

Current tests cover handlers and domain mapping. Add real endpoint tests for the main API flows.

- `/auth/me` without Authorization header -> `MISSING_AUTH_HEADER`.
- `/auth/refresh` without cookie -> `MISSING_REFRESH_TOKEN`.
- `/search/click` with missing dataset -> `DATASET_NOT_FOUND`.
- Invalid request body -> `VALIDATION_ERROR`.
- Any protected endpoint with inactive user -> `ACCOUNT_INACTIVE`.

## 7. Improve Observability

Production error handling should be easy to search, alert on, and correlate.

- Log `error_code`, `status_code`, `path`, `method`, and request id/correlation id.
- Log 4xx as warning and 5xx as error.
- Never log secrets, tokens, passwords, or raw auth headers.
- Add metrics grouped by `error_code` and HTTP status when monitoring is introduced.

## 8. Optional: Separate Domain Errors From HTTP Status

The current approach keeps `status_code` on exceptions. This is pragmatic and works well with FastAPI.

A stricter architecture could later split responsibilities:

- domain exceptions contain only `error_code`, `message`, and `details`
- a separate mapper converts domain errors to HTTP status codes

This is optional. The current design is acceptable for this project unless transport independence becomes important.

## Recommended Order

1. Add endpoint-level tests.
2. Add OpenAPI response metadata.
3. Add task/worker error serialization.
4. Write enrichment logs with stable `error_code`.
5. Tighten remaining broad `except Exception` blocks where concrete types are available.
6. Add repository/database boundary domain errors where they represent business scenarios.
