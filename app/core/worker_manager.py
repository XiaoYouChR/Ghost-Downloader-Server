import asyncio
from typing import Dict, Any, List, Optional, Callable, Awaitable
from loguru import logger

from sdk.ghost_downloader_sdk.models import TaskStage, TaskStatus
from sdk.ghost_downloader_sdk.interfaces import IWorker, IWorkerContext

ProgressCallback = Callable[[str, float], Awaitable[None]]
CompletionCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]
ErrorCallback = Callable[[str, Exception], Awaitable[None]]
SaveResumeDataCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]

class _WorkerContext(IWorkerContext):
    """
    An internal implementation of the IWorkerContext interface.
    It holds references to the CoreEngine's callbacks.
    """
    _config: Dict[str, Any]
    _stageId: str
    _cleanupOnCancel: bool = False

    _onProgress: ProgressCallback
    _onCompletion: CompletionCallback
    _onError: ErrorCallback
    _onSaveResumeData: SaveResumeDataCallback

    def __init__(self, stageId: str, config: Dict[str, Any], **callbacks):
        self._stageId = stageId
        self._config = config
        self._onProgress = callbacks['onProgress']
        self._onCompletion = callbacks['onCompletion']
        self._onError = callbacks['onError']
        self._onSaveResumeData = callbacks['onSaveResumeData']

    @property
    def config(self) -> Dict[str, Any]:
        return self._config

    async def reportProgress(self, progress: float, descriptionKey: Optional[str] = None):
        # TODO: descriptionKey 的处理逻辑
        await self._onProgress(self._stageId, progress)

    async def reportCompletion(self, result: Dict[str, Any]):
        await self._onCompletion(self._stageId, result)

    async def reportError(self, error: Exception):
        await self._onError(self._stageId, error)

    async def saveResumeData(self, resumeData: Dict[str, Any]):
        await self._onSaveResumeData(self._stageId, resumeData)

    def setCleanupFlag(self, cleanup: bool):
        self._cleanupOnCancel = cleanup

    def shouldCleanupOnCancel(self) -> bool:
        return self._cleanupOnCancel


class WorkerManager:
    """
    Manages the execution of all tasks by handling concurrency,
    instantiating workers, and monitoring their lifecycle.
    """
    _maxConcurrentWorkers: int
    _semaphore: asyncio.Semaphore

    _runningTasks: Dict[str, tuple[asyncio.Task, _WorkerContext]]

    # CoreEngine 提供的回调函数
    _onProgress: ProgressCallback
    _onCompletion: CompletionCallback
    _onError: ErrorCallback
    _onSaveResumeData: SaveResumeDataCallback

    def __init__(self, maxConcurrentWorkers: int = 4):
        if maxConcurrentWorkers <= 0:
            raise ValueError("maxConcurrentWorkers must be a positive integer.")

        self._maxConcurrentWorkers = maxConcurrentWorkers
        self._semaphore = asyncio.Semaphore(self._maxConcurrentWorkers)
        _runningTasks: Dict[str, tuple[asyncio.Task, _WorkerContext]] = {}

        logger.info(f"WorkerManager initialized with a concurrency limit of {self._maxConcurrentWorkers}.")

    def setCallbacks(self, **callbacks):
        """Injects callback methods from the CoreEngine."""
        self._onProgress = callbacks.get('onProgress')
        self._onCompletion = callbacks.get('onCompletion')
        self._onError = callbacks.get('onError')
        self._onSaveResumeData = callbacks.get('onSaveResumeData')

    async def submitTask(self, stage: TaskStage, workerImplementation: IWorker, config: Dict[str, Any]):
        logger.info(f"Submitting stage '{stage.stageId}' of type '{stage.workerType}'.")
        await self._semaphore.acquire()
        logger.debug(f"Concurrency slot acquired for stage '{stage.stageId}'.")

        context = _WorkerContext(
            stage.stageId, config,
            onProgress=self._onProgress,
            onCompletion=self._onCompletion,
            onError=self._onError,
            onSaveResumeData=self._onSaveResumeData
        )

        task = asyncio.create_task(
            self._executeWrapper(stage, workerImplementation, context)
        )

        self._runningTasks[stage.stageId] = (task, context)

    async def cancelTask(self, stageId: str, cleanup: bool = False):
        """
        Cancels a running task associated with a stageId.
        """
        if stageId in self._runningTasks:
            task, context = self._runningTasks[stageId]

            if not task.done():
                logger.info(f"Cancelling execution for stage '{stageId}'...")

                context.setCleanupFlag(cleanup)
                task.cancel()

                try:
                    await task
                except asyncio.CancelledError:
                    logger.success(f"Execution for stage '{stageId}' was successfully cancelled.")

            # 从映射中移除
            del self._runningTasks[stageId]
        else:
            logger.warning(f"Attempted to cancel stage '{stageId}', but it was not found in the running tasks.")

    async def _executeWrapper(self, stage: TaskStage, worker: IWorker, config: Dict[str, Any]):
        """
        A private wrapper that runs the worker's execute method, handles
        completion/errors, and releases the semaphore.
        """
        stageId = stage.stageId
        try:
            context = _WorkerContext(
                stageId,
                config,
                onProgress=self._onProgress,
                onCompletion=self._onCompletion,
                onError=self._onError,
                onSaveResumeData=self._onSaveResumeData
            )

            # 核心执行步骤
            await worker.execute(stage.instructionPayload, context)

            # 如果 worker 的 execute 方法正常返回而没有调用 onCompletion，我们在这里补上
            # await self._onCompletion(stageId, {})
            # worker 需要处理 context.shouldCleanupOnCancel 后重新 raise CancelledError

        except asyncio.CancelledError:
            logger.info(f"Worker for stage '{stageId}' was cancelled.")
            await self._onError(stageId, asyncio.CancelledError(f"Stage was cancelled by user. Cleanup: {context.shouldCleanupOnCancel()}"))

        except Exception as e:
            # 捕获 worker 执行过程中的所有其他异常
            logger.error(f"An unexpected error occurred in worker for stage '{stageId}': {e}", exc_info=True)
            await self._onError(stageId, e)

        finally:
            # 无论成功、失败还是取消，都必须释放信号量！
            self._semaphore.release()
            logger.debug(f"Concurrency slot released for stage '{stageId}'.")

            # 从正在运行的任务映射中移除自己
            if stageId in self._runningTasks:
                del self._runningTasks[stageId]
