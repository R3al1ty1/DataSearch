from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from lib.core.container import container
from lib.api.handlers.router import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for FastAPI application."""
    logger = container.logger
    logger.info(f"🚀 Starting {container.settings.PROJECT_NAME}...")

    try:
        container.db.init()

        async with container.db.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

        logger.info(f"✅ Database connected: {container.settings.POSTGRES_HOST}")
    except Exception as e:
        logger.critical(f"❌ Database connection failed: {e}")
        raise e

    yield

    logger.info("🛑 Shutting down application...")

    await container.db.close()


def create_app() -> FastAPI:
    """Creates and configures the FastAPI application."""
    app = FastAPI(
        title=container.settings.PROJECT_NAME,
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs" if container.settings.DEBUG else None,
        redoc_url=None,
        openapi_url=f"{container.settings.API_V1_STR}/openapi.json"
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix=container.settings.API_V1_STR)

    return app


app = create_app()
