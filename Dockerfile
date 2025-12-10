FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --system --no-dev

COPY . .

CMD ["uvicorn", "lib.main:app", "--host", "0.0.0.0", "--port", "8000"]