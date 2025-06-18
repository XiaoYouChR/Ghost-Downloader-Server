import asyncio
import sys
from pathlib import Path

import typer
from loguru import logger
import uvicorn

from app.infrastructure.database import Database
from app.infrastructure.plugin.plugin_service import UnifiedPluginService
from app.infrastructure.config_service import ConfigService
from app.core.engine import CoreEngine
from app.core.worker_manager import WorkerManager
from app.core.middleware import Middleware
from app.api.gateway import create_app

def setupLogging(log_level: str):
    """Configures the Loguru logger."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level.upper(),
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.info(f"Logger configured with level: {log_level}")

async def run_server(
    host: str,
    port: int,
    dataDir: Path,
    max_concurrent_workers: int,
    log_level: str
):
    """
    Initializes and runs all services for the Ghost Downloader Server.
    """
    # 设置日志和路径
    setupLogging(log_level)
    dataDir.mkdir(parents=True, exist_ok=True)
    dbPath = dataDir / "ghost_downloader.db"
    featuresDir = dataDir / "features"
    featuresDir.mkdir(exist_ok=True)

    logger.info(f"Using data directory: {dataDir}")

    # b. 初始化所有后台服务 (严格按依赖顺序)
    logger.info("Initializing backend services...")
    
    db = Database(dbPath=str(dbPath))
    await db.connect()

    pluginService = UnifiedPluginService(featureDir=str(featuresDir))
    pluginService.loadPlugins()

    configService = ConfigService(db=db, pluginService=pluginService)
    await configService.initialize()

    maxWorkers = configService.getEffectiveConfig("server.max_concurrent_workers") or max_concurrent_workers

    workerManager = WorkerManager(maxConcurrentWorkers=maxWorkers)
    coreEngine = CoreEngine(
        db=db, 
        workerManager=workerManager, 
        pluginService=pluginService, 
        configService=configService
    )
    middleware = Middleware(pluginService=pluginService, coreEngine=coreEngine)
    
    # 创建并配置 FastAPI 应用
    logger.info("Creating FastAPI application...")
    fastApiApp = create_app(
        middleware=middleware,
        coreEngine=coreEngine,
        pluginService=pluginService,
        configService=configService
    )
    
    # 启动 FastAPI 服务器
    config = uvicorn.Config(fastApiApp, host=host, port=port, log_config=None)
    server = uvicorn.Server(config)
    
    logger.info(f"Starting API server at http://{host}:{port}")
    
    try:
        await server.serve()
    finally:
        logger.info("Shutting down services...")
        await db.close()
        logger.info("Application shut down gracefully.")


cliApp = typer.Typer(name="Ghost Downloader Server")

@cliApp.command()
def start(
    host: str = typer.Option("127.0.0.1", help="The host to bind the API server to."),
    port: int = typer.Option(8000, help="The port to run the API server on."),
    data_dir: Path = typer.Option(
        Path.home() / ".ghost_downloader",
        help="The root directory for all application data (database, plugins).",
        writable=True, resolve_path=True,
    ),
    max_concurrent_workers: int = typer.Option(
        4, "--workers", "-w",
        help="Maximum number of concurrent workers for executing tasks."
    ),
    log_level: str = typer.Option(
        "INFO", "--log-level",
        help="Set the logging level (DEBUG, INFO, WARNING, ERROR)."
    )
    ):

    asyncio.run(run_server(host, port, data_dir, max_concurrent_workers, log_level))

if __name__ == "__main__":
    isCompiled = "__compiled__" in globals()
    
    if isCompiled:
        cliApp()
    else:
        logger.info("No CLI command detected, running in direct/debug mode.")

        debug_data_dir = Path("./.debug_data").resolve()

        asyncio.run(run_server(
            host="127.0.0.1",
            port=8001, # 使用一个不同的端口以避免与生产实例冲突
            dataDir=debug_data_dir,
            max_concurrent_workers=2,
            log_level="DEBUG"
        ))
