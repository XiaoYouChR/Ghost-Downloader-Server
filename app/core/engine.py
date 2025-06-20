import asyncio
from typing import Dict, Any, List, Optional, Callable
from loguru import logger

from sdk.ghost_downloader_sdk.models import (
    Task, TaskStage, StageDefinition, CompletedTaskContext,
    TaskStatus, OverallTaskStatus
)
from sdk.ghost_downloader_sdk.interfaces import IWorker

# 导入依赖的基础设施服务
from ..infrastructure.database import Database
from ..infrastructure.plugin.plugin_service import UnifiedPluginService
from ..infrastructure.config_service import ConfigService
from .worker_manager import WorkerManager

class CoreEngine:
    """
    The central state machine and scheduler for all tasks and stages.
    It is the sole authority for database writes related to task lifecycles,
    and it orchestrates the entire task flow from creation to completion.
    """
    _db: Database
    _workerManager: WorkerManager
    _pluginService: UnifiedPluginService
    _configService: ConfigService
    _eventListeners: Dict[str, List[Callable]]
    _taskLocks: Dict[str, asyncio.Lock] # Per-task lock for scheduling atomicity

    def __init__(self,
                 db: Database,
                 workerManager: WorkerManager,
                 pluginService: UnifiedPluginService,
                 configService: ConfigService):

        self._db = db
        self._workerManager = workerManager
        self._pluginService = pluginService
        self._configService = configService

        self._eventListeners = {}
        self._taskLocks = {}

        # 通过依赖注入设置回调，这是清晰的单向数据流
        self._workerManager.setCallbacks(
            onProgress=self.onStageProgress,
            onCompletion=self.onStageCompletion,
            onError=self.onStageError,
            OnSaveResumeData = self.onStageSaveResumeData
        )

    # --- Event System ---
    def on(self, eventName: str, callback: Callable):
        self._eventListeners.setdefault(eventName, []).append(callback)

    async def _emit(self, eventName: str, *args, **kwargs):
        if eventName in self._eventListeners:
            listeners = self._eventListeners[eventName][:] # Create a copy
            for callback in listeners:
                try:
                    await callback(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in event handler for '{eventName}': {e}", exc_info=True)

    def _getTaskLock(self, taskId: str) -> asyncio.Lock:
        """Gets or creates a lock for a specific task ID to ensure atomic operations."""
        if taskId not in self._taskLocks:
            self._taskLocks[taskId] = asyncio.Lock()
        return self._taskLocks[taskId]

    # --- Public Control API (called by Middleware) ---
    async def getAllTasks(self) -> List[Task]:
        """
        Retrieves a list of all parent tasks.
        This method is designed for a high-level overview, so it does not
        include detailed stages or metadata.
        """
        logger.debug("Fetching all tasks for API request.")
        tasksFromDb = await self._db.getAllTasks()
        return tasksFromDb

    async def getTask(self, taskId: str) -> Optional[Task]:
        """
        Retrieves a single parent task object by its ID, without its details.
        """
        logger.debug(f"Fetching task '{taskId}' for API request.")
        task = await self._db.getTask(taskId)
        return task

    async def getTaskWithDetails(self, taskId: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single task with all its associated stages and metadata.
        Returns a dictionary defectos suitable for direct JSON serialization.
        """
        logger.debug(f"Fetching full details for task '{taskId}'.")

        taskDetailsDict = await self._db.getTaskWithDetails(taskId)

        if not taskDetailsDict:
            return None

        return taskDetailsDict

    async def createTask(self, title: str, metadata: Dict[str, Any] = {}) -> Task:
        """Creates a new parent Task and persists it."""
        taskModel = Task(title=title)
        await self._db.createTask(taskModel)
        if metadata:
            await self._db.upsertMetadata(taskModel.taskId, "task:initial_metadata", metadata)
        logger.info(f"Created new task '{title}' with ID: {taskModel.taskId}")
        return taskModel

    async def addStagesToTask(self, taskId: str, stageDefs: List[StageDefinition]):
        """Adds new stages to an existing task and triggers the scheduler."""
        task = await self._db.getTask(taskId)
        if not task:
            raise ValueError(f"Task with ID '{taskId}' not found.")

        currentMaxIndex = await self._db.getMaxStageIndex(taskId) or -1

        newStages = [
            TaskStage(
                taskId=taskId,
                stageIndex=currentMaxIndex + 1 + i,
                displayIntent=stageDef.displayIntent,
                workerType=stageDef.workerType,
                instructionPayload=stageDef.payload,
            ) for i, stageDef in enumerate(stageDefs)
        ]

        await self._db.addStages(newStages)
        logger.info(f"Added {len(newStages)} new stage(s) to task '{taskId}'.")
        # 创建一个后台任务来运行调度器，不会阻塞当前流程
        asyncio.create_task(self.scheduleTask(taskId))

    async def pauseTask(self, taskId: str):
        logger.info(f"Requesting to pause task '{taskId}'...")
        async with self._getTaskLock(taskId):
            await self._db.updateTaskStatus(taskId, OverallTaskStatus.PAUSED)
            activeStages = await self._db.getActiveStagesForTask(taskId)
            pauseJobs = [self._workerManager.cancelTask(stage.stageId) for stage in activeStages]
            await asyncio.gather(*pauseJobs, return_exceptions=True) # 忽略 Worker 停止时的错误
        logger.success(f"Task '{taskId}' paused.")
        await self._emit("taskUpdated", taskId)

    async def resumeTask(self, taskId: str):
        logger.info(f"Resuming task '{taskId}'...")
        async with self._getTaskLock(taskId):
            await self._db.updateTaskStatus(taskId, OverallTaskStatus.RUNNING)
        await self._emit("taskUpdated", taskId)
        asyncio.create_task(self.scheduleTask(taskId))

    async def cancelTask(self, taskId: str, cleanup: bool):
        logger.info(f"Requesting to cancel task '{taskId}' with cleanup={cleanup}...")
        async with self._getTaskLock(taskId):
            await self._db.updateTaskStatus(taskId, OverallTaskStatus.FAILED) # 'FAILED' 是取消后的最终状态
            activeStages = await self._db.getActiveStagesForTask(taskId)
            cancelJobs = [self._workerManager.cancelTask(stage.stageId, cleanup=cleanup) for stage in activeStages]
            await asyncio.gather(*cancelJobs, return_exceptions=True)
        logger.success(f"Task '{taskId}' cancelled.")
        await self._emit("taskUpdated", taskId)

    # --- Core Scheduler ---

    async def scheduleTask(self, taskId: str):
        """The main scheduling loop, simplified and robust."""
        async with self._getTaskLock(taskId):
            task = await self._db.getTask(taskId)
            if not task or task.overallStatus != OverallTaskStatus.RUNNING:
                return

            stagesToRun = await self._db.getNextRunnableStages(taskId)
            if not stagesToRun:
                if not await self._db.hasPendingStages(taskId):
                    await self._completeTask(task)
                return

            logger.info(f"Scheduler: Found {len(stagesToRun)} stage(s) to run for task '{taskId}'.")
            await self._db.updateTaskStatus(taskId, task.overallStatus, stagesToRun[0].stageId)

            for stage in stagesToRun:
                workerToUse = self._pluginService.findBestWorker(stage.workerType, stage.instructionPayload)
                if not workerToUse:
                    errMsg = f"No suitable worker for stage '{stage.stageId}' (type: '{stage.workerType}')."
                    logger.error(errMsg)
                    await self.onStageError(stage.stageId, RuntimeError(errMsg))
                    continue

                config = self._prepareConfigForWorker(task, workerToUse)
                await self._db.updateStage(stage.stageId, {'status': TaskStatus.RUNNING.value})
                await self._workerManager.submitTask(stage, workerToUse, config)

            # 在提交完一批任务后，再统一发一次更新通知
            await self._emit("taskUpdated", taskId)


    # --- Callbacks from WorkerManager ---

    async def onStageProgress(self, stageId: str, progress: float):
        await self._db.updateStage(stageId, {'progress': progress})
        stage = await self._db.getStage(stageId)
        if stage:
            await self._emit("stageUpdated", stageId, stage.taskId)

    async def onStageCompletion(self, stageId: str, result: Dict[str, Any]):
        logger.success(f"Stage '{stageId}' completed.")
        stage = await self._db.getStage(stageId)
        if stage:
            await self._db.updateStage(stageId, {'status': TaskStatus.COMPLETED.value, 'progress': 1.0})
            if result:
                await self._db.upsertMetadata(stageId, "worker:result", result)
            await self._emit("stageUpdated", stageId, stage.taskId)
            asyncio.create_task(self.scheduleTask(stage.taskId))

    async def onStageError(self, stageId: str, error: Exception):
        logger.error(f"Stage '{stageId}' failed: {error}")
        stage = await self._db.getStage(stageId)
        if stage:
            if isinstance(error, asyncio.CancelledError):
                await self._db.updateStage(stageId, {'status': TaskStatus.PAUSED.value})
                await self._db.updateTaskStatus(stage.taskId, OverallTaskStatus.PAUSED)
            else:
                await self._db.updateStage(stageId, {'status': TaskStatus.FAILED.value})
                await self._db.upsertMetadata(stageId, "worker:last_error", {"error": str(error)})
                await self._db.updateTaskStatus(stage.taskId, OverallTaskStatus.FAILED)

            await self._emit("stageUpdated", stageId, stage.taskId)
            await self._emit("taskUpdated", stage.taskId)

    async def onStageSaveResumeData(self, stageId: str, resumeData: Dict[str, Any]):
        """Callback to persist a worker's resumable state."""
        logger.debug(f"Saving resume data for stage '{stageId}'.")
        await self._db.upsertMetadata(stageId, "worker:resume_data", resumeData)

    # --- Internal Helpers ---
    def _prepareConfigForWorker(self, task: Task, worker: IWorker) -> Dict[str, Any]:
        effectiveConfig: Dict[str, Any] = {}
        workerClass = type(worker)
        relevantKeys = workerClass.getRequiredConfigKeys()
        if not relevantKeys: return effectiveConfig
        for key in relevantKeys:
            effectiveConfig[key] = self._configService.getEffectiveConfig(key, task=task)
        return effectiveConfig

    async def _completeTask(self, task: Task):
        """Finalizes a task when all its stages are complete."""
        logger.success(f"All stages for task '{task.taskId}' are complete. Finalizing.")
        await self._db.updateTaskStatus(task.taskId, OverallTaskStatus.COMPLETED)
        await self._emit("taskUpdated", task.taskId)

        context = await self.getCompletedTaskContext(task.taskId)
        if context:
            await self._emit("taskCompletionWorkflow", context)

    async def getCompletedTaskContext(self, taskId: str) -> Optional[CompletedTaskContext]:
        taskDetailsDict = await self._db.getTaskWithDetails(taskId)
        if not taskDetailsDict: return None

        parentTask = Task(**{k: v for k, v in taskDetailsDict.items() if k not in ['stages', 'metadata']})
        allStages = taskDetailsDict.get('stages', [])
        completedStages = [stage for stage in allStages if stage.status == TaskStatus.COMPLETED]

        return CompletedTaskContext(parentTask=parentTask, completedStages=completedStages)
