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


class WorkerManager:
    """
    Manages the execution of all tasks by handling concurrency,
    instantiating workers, and monitoring their lifecycle.
    """
    _maxConcurrentWorkers: int
    _semaphore: asyncio.Semaphore

    # 映射：stageId -> asyncio.Task
    _runningTasks: Dict[str, asyncio.Task]

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
        self._runningTasks = {}
        logger.info(f"WorkerManager initialized with a concurrency limit of {self._maxConcurrentWorkers}.")

    def setCallbacks(self, **callbacks):
        """Injects callback methods from the CoreEngine."""
        self._onProgress = callbacks.get('onProgress')
        self._onCompletion = callbacks.get('onCompletion')
        self._onError = callbacks.get('onError')
        self._onSaveResumeData = callbacks.get('onSaveResumeData')

    async def submitTask(self, stage: TaskStage, workerImplementation: IWorker, config: Dict[str, Any]):
        """
        Submits a stage for execution. It acquires a concurrency slot and
        starts the worker in a new asyncio Task.
        """
        logger.info(f"Submitting stage '{stage.stageId}' of type '{stage.workerType}' to the execution queue.")

        # 使用信号量来等待一个可用的“执行槽”
        await self._semaphore.acquire()

        logger.debug(f"Concurrency slot acquired for stage '{stage.stageId}'. Starting worker.")

        # 运行一个包裹了 worker 执行逻辑的协程任务
        task = asyncio.create_task(
            self._executeWrapper(stage, workerImplementation, config)
        )

        # 记录这个正在运行的任务，以便取消
        self._runningTasks[stage.stageId] = task

    async def cancelTask(self, stageId: str, cleanup: bool = False):
        """
        Cancels a running task associated with a stageId.
        """
        if stageId in self._runningTasks:
            task = self._runningTasks[stageId]

            if not task.done():
                logger.info(f"Cancelling execution for stage '{stageId}'...")
                task.cancel() # 发送取消请求

                try:
                    await task # 等待任务响应取消
                except asyncio.CancelledError:
                    logger.success(f"Execution for stage '{stageId}' was successfully cancelled.")
                # 注意：Worker 内部需要捕获 CancelledError 来执行清理逻辑

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
            # 创建上下文对象，并注入回调
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
            # (这是一个健壮性措施)
            # await self._onCompletion(stageId, {})

        except asyncio.CancelledError:
            logger.info(f"Worker for stage '{stageId}' was cancelled.")
            # 当被取消时，我们将其标记为 PAUSED，以便可以恢复
            await self._db.updateStage(stageId, {'status': TaskStatus.PAUSED.value})
            await self._onProgress(stageId, 0.0) # Reset progress for UI if needed

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
