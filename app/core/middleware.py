# app/core/middleware.py

from typing import Dict, Any, List, Optional
from loguru import logger

from sdk.ghost_downloader_sdk.models import Task, StageDefinition, CompletedTaskContext

from ..infrastructure.plugin.plugin_service import UnifiedPluginService
from .engine import CoreEngine

class Middleware:
    def __init__(self, pluginService: UnifiedPluginService, coreEngine: CoreEngine):
        self._pluginService = pluginService
        self._coreEngine = coreEngine
        self._setupEventListeners()
        logger.info("Middleware initialized and ready.")

    def _setupEventListeners(self):
        self._coreEngine.on("task_completed", self.onTaskCompleted)

    # --- API-Facing Methods ---

    async def createNewTaskFromUrl(self, url: str, settingsOverrides: Dict[str, Any]) -> Task:
        """
        The main entry point for creating a new task from a URL.
        """
        logger.info(f"Attempting to create task from URL: {url}")

        # 1. 查找 Parser
        parser = self._pluginService.findParserForUrl(url)
        if not parser:
            # 明确地处理无法解析的情况
            logger.error(f"No suitable parser found for URL: {url}")
            raise ValueError(f"Unsupported URL type. No active plugin can handle this link.")

        logger.debug(f"Using parser '{type(parser).__name__}' for URL.")

        # 2. 从 Parser 获取初始阶段
        # 增加错误处理，因为插件的实现可能是不可靠的
        try:
            initialStageDefs: List[StageDefinition] = await parser.getInitialStages(url)
            if not initialStageDefs:
                raise ValueError("Parser returned an empty list of initial stages.")
        except Exception as e:
            logger.error(f"Parser '{type(parser).__name__}' failed to get initial stages for '{url}': {e}", exc_info=True)
            raise IOError(f"Failed to parse URL due to a plugin error.")

        # 3. 准备父任务的元数据
        parserPackId = self._pluginService.findPackIdForAbility(parser)
        taskMetadata = {
            'sourceUrl': url,
            'configOverrides': settingsOverrides,
            'sourcePackId': parserPackId
        }

        # 从第一个阶段的描述意图中获取一个合适的标题
        taskTitle = initialStageDefs[0].displayIntent.context.get('title', url)

        # 4. 创建任务和阶段
        parentTask = await self._coreEngine.createTask(title=taskTitle, metadata=taskMetadata)
        await self._coreEngine.addStagesToTask(parentTask.taskId, initialStageDefs)

        logger.success(f"Successfully created task '{parentTask.taskId}' ('{taskTitle}') with {len(initialStageDefs)} initial stage(s).")

        # 5. 返回完整的父任务对象
        return await self._coreEngine.getTaskWithDetails(parentTask.taskId)

    # --- Event Handler Methods ---

    async def onTaskCompleted(self, context: CompletedTaskContext):
        """
        Handles the completion of a task (or a set of its stages),
        and orchestrates the execution of subsequent workflows.
        """
        taskId = context.parentTask.taskId
        logger.info(f"Handling completion for task '{taskId}'. Searching for applicable workflows.")

        # 1. 查找匹配的工作流
        matchedWorkflows = self._pluginService.findWorkflowsForTask(context)
        if not matchedWorkflows:
            logger.info(f"No workflows found for completed task '{taskId}'. Process ends.")
            return

        logger.info(f"Found {len(matchedWorkflows)} workflow(s) for task '{taskId}'. Generating plans...")

        # 2. 生成并合并所有工作流的计划
        allNewStageDefs: List[StageDefinition] = []
        for workflow in matchedWorkflows:
            try:
                # 每个工作流都基于最新的上下文生成计划
                plan = await workflow.generatePlan(context)
                if plan:
                    logger.debug(f"Workflow '{type(workflow).__name__}' generated {len(plan)} new stage(s).")
                    allNewStageDefs.extend(plan)
            except Exception as e:
                # 记录错误，但继续执行其他工作流
                logger.error(f"Workflow '{type(workflow).__name__}' failed for task '{taskId}': {e}", exc_info=True)

        # 3. 将新阶段添加到任务中
        if allNewStageDefs:
            logger.info(f"Appending {len(allNewStageDefs)} new stage(s) to task '{taskId}'.")
            await self._coreEngine.addStagesToTask(taskId, allNewStageDefs)
        else:
            logger.info(f"Workflows generated no new stages for task '{taskId}'.")
