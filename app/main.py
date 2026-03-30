from __future__ import annotations

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers.business import router as business_router
from app.routers.embeddings import router as embeddings_router
from app.routers.faqs import router as faqs_router
from app.routers.health import router as health_router
from app.routers.messaging import router as messaging_router
from app.routers.products import router as products_router
from app.routers.search import router as search_router
from app.utils.logging import setup_logging


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    setup_logging(settings.log_level)
    logger.info("Starting %s in %s mode", settings.app_name, settings.environment)
    _.state.public_webhook_base_url = settings.public_webhook_base_url
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="Business-scoped RAG backend for ZakBot and n8n workflows.",
        lifespan=lifespan,
    )
    cors_allow_origins = [
        origin.strip()
        for origin in settings.cors_allow_origins.split(",")
        if origin.strip()
    ]
    if cors_allow_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_allow_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        logger.warning("Validation failed for %s: %s", request.url.path, exc.errors())
        return JSONResponse(
            status_code=422,
            content={
                "detail": "Validation error",
                "errors": exc.errors(),
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.exception("Unhandled error on %s", request.url.path, exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    app.include_router(health_router)
    app.include_router(business_router)
    app.include_router(messaging_router)
    app.include_router(products_router)
    app.include_router(faqs_router)
    app.include_router(embeddings_router)
    app.include_router(search_router)
    return app


app = create_app()
