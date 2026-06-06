# API Errors

All API errors use the same JSON shape:

```json
{
  "error_code": "DATASET_NOT_FOUND",
  "message": "Dataset not found",
  "details": {
    "resource": "Dataset",
    "identifier": "..."
  }
}
```

## Fields

| Field | Type | Description |
| --- | --- | --- |
| `error_code` | string | Stable machine-readable code from `lib.core.error_codes.ErrorCode`. |
| `message` | string | Human-readable message safe to return to clients. |
| `details` | object or null | Optional structured context for client handling and debugging. |

## Common Codes

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | `PASSWORD_VALIDATION_FAILED` | Password does not satisfy policy. |
| 400 | `INVALID_SEARCH_QUERY` | Search request is invalid. |
| 401 | `AUTHENTICATION_ERROR` | Generic authentication failure. |
| 401 | `MISSING_AUTH_HEADER` | Bearer authorization header is missing. |
| 401 | `MISSING_REFRESH_TOKEN` | Refresh token cookie is missing. |
| 401 | `INVALID_CREDENTIALS` | Email or password is invalid. |
| 401 | `TOKEN_EXPIRED` | Token is expired. |
| 401 | `TOKEN_INVALID` | Token is malformed or not accepted. |
| 401 | `TOKEN_BLACKLISTED` | Token has been revoked. |
| 403 | `ACCOUNT_INACTIVE` | User account is inactive. |
| 403 | `INSUFFICIENT_PERMISSIONS` | User role is not allowed to perform the action. |
| 404 | `RESOURCE_NOT_FOUND` | Generic resource not found. |
| 404 | `DATASET_NOT_FOUND` | Dataset was not found. |
| 404 | `SEARCH_LOG_NOT_FOUND` | Search log referenced by click tracking was not found. |
| 409 | `USER_ALREADY_EXISTS` | Email is already registered. |
| 409 | `DATASET_CONFLICT` | Dataset source identity already exists. |
| 422 | `VALIDATION_ERROR` | Request validation failed. |
| 429 | `RATE_LIMIT_EXCEEDED` | Login or request limit was exceeded. |
| 502 | `EXTERNAL_SERVICE_ERROR` | Upstream service failed. |
| 503 | `TASK_QUEUE_ERROR` | Background task could not be queued. |
| 429 | `ENRICHMENT_RATE_LIMITED` | External enrichment source rate limited the request. |
| 502 | `ENRICHMENT_SOURCE_ERROR` | External enrichment source failed. |
| 500 | `ENRICHMENT_PROCESSING_ERROR` | Enrichment data could not be processed or stored. |
| 503 | `EMBEDDING_MODEL_LOAD_FAILED` | Embedding model could not be loaded. |
| 503 | `EMBEDDING_ENCODING_FAILED` | Embedding generation failed. |
| 503 | `EMBEDDING_PERSISTENCE_FAILED` | Embedding could not be saved. |
| 500 | `INTERNAL_ERROR` | Unexpected server error. |

Validation errors include field-level details:

```json
{
  "error_code": "VALIDATION_ERROR",
  "message": "Request validation failed",
  "details": {
    "fields": [
      {"field": "body.query", "message": "Field required"}
    ]
  }
}
```

Error logs include `error_code`, HTTP status, request path, method, and request id
from `X-Request-ID` or `X-Correlation-ID` when present. The same id is echoed in
the `X-Request-ID` response header.
