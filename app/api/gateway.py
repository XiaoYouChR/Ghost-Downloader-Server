# app/api/gateway.py

import time
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from app.api.routers import task_router, plugin_router, config_router
# 导入我们的核心服务类型，用于类型注解和状态存储
from app.core.middleware import Middleware
from app.core.engine import CoreEngine
from app.infrastructure.plugin.plugin_service import UnifiedPluginService
from app.infrastructure.config_service import ConfigService

# 导入我们即将创建的路由模块
# from .routers import task_router, plugin_router, config_router

def create_app(
    middleware: Middleware,
    coreEngine: CoreEngine,
    pluginService: UnifiedPluginService,
    configService: ConfigService
) -> FastAPI:
    """
    Creates and configures the main FastAPI application instance.
    Dependencies (core services) are injected here and attached to the app state.
    """

    app = FastAPI(
        title="Ghost Downloader Server API",
        description="The backend API service for Ghost Downloader, providing task, plugin, and configuration management.",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # --- 将核心服务实例存储在 app.state 中 ---
    app.state.middleware = middleware
    app.state.coreEngine = coreEngine
    app.state.pluginService = pluginService
    app.state.configService = configService


    # 中间件是按顺序处理的，日志和错误处理应该放在前面
    @app.middleware("http")
    async def log_requests_and_handle_errors(request: Request, call_next):
        """
        A single middleware for logging requests and handling all unexpected errors.
        This provides a robust top-level error boundary.
        """
        request_id = request.headers.get("X-Request-ID", "N/A")
        logger.info(f"Request started: {request_id} {request.method} {request.url.path}")
        start_time = time.time()

        try:
            response = await call_next(request)
            process_time = (time.time() - start_time) * 1000
            logger.info(f"Request finished: {request_id} {response.status_code} (took {process_time:.2f}ms)")
            return response
        except Exception as e:
            # 捕获所有未被路由层处理的、意料之外的异常
            logger.error(f"Unhandled exception for request {request_id}: {e}", exc_info=True)
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"error": "Internal Server Error", "detail": "An unexpected error occurred on the server."},
            )

    # 配置 CORS (Cross-Origin Resource Sharing)
    # 这对于允许来自不同域的浏览器插件或Web UI访问API至关重要
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(task_router.router)
    app.include_router(plugin_router.router)
    app.include_router(config_router.router)

    @app.get("/", tags=["Root"])
    async def read_root():
        """A simple health check endpoint."""
        return {"status": "ok", "message": "Welcome to Ghost Downloader Server!"}

    @app.on_event("shutdown")
    async def shutdown_event():
        logger.info("FastAPI application shutdown...")

    return app
