from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any, Optional
from loguru import logger
from pydantic import BaseModel

from app.core.middleware import Middleware
from app.core.engine import CoreEngine
from app.infrastructure.config_service import ConfigService
from app.api.deps import get_middleware, get_core_engine, get_config_service

from sdk.ghost_downloader_sdk.models import Task

# --- 请求体模型 ---
class CreateTaskRequest(BaseModel):
    url: str
    configOverrides: Dict[str, Any] = {}

class UpdateTaskConfigRequest(BaseModel):
    key: str
    value: Any

# --- API 路由 ---
router = APIRouter(prefix="/tasks", tags=["Tasks"])

@router.post(
    "/url", 
    response_model=Task, 
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a New Task from a URL"
)
async def create_task_from_url(
    request: CreateTaskRequest,
    middleware: Middleware = Depends(get_middleware)
):
    """
    Submits a new URL to be parsed and creates a new download task.
    This is an asynchronous operation; the response returns the initial
    task object, and its status should be tracked via other endpoints.
    """
    try:
        task = await middleware.createNewTaskFromUrl(request.url, request.configOverrides)
        # FastAPI 会自动将返回的 Pydantic Task 对象序列化为 JSON
        return task
    except ValueError as e:
        logger.warning(f"Failed to create task from URL '{request.url}': {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except IOError as e:
        logger.error(f"Plugin I/O error for URL '{request.url}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Plugin error: {e}")
    except Exception:
        logger.exception(f"Unexpected error creating task from URL '{request.url}'.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")

@router.get(
    "/", 
    response_model=List[Task], 
    summary="List All Tasks"
)
async def list_all_tasks(
    coreEngine: CoreEngine = Depends(get_core_engine)
):
    """Retrieves a list of all current parent tasks."""
    tasks = await coreEngine.getAllTasks()
    return tasks

@router.get(
    "/{taskId}", 
    response_model=Dict[str, Any], # 返回一个包含 Task 和 Stages 的复杂字典
    summary="Get Task Details"
)
async def get_task_details(
    taskId: str,
    coreEngine: CoreEngine = Depends(get_core_engine)
):
    """
    Retrieves detailed information about a single task, including all its
    stages and metadata.
    """
    task_details = await coreEngine.getTaskWithDetails(taskId)
    if not task_details:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task with ID '{taskId}' not found.")
    return task_details

@router.post(
    "/{taskId}/pause", 
    response_model=Task, 
    summary="Pause a Task"
)
async def pause_task(
    taskId: str,
    coreEngine: CoreEngine = Depends(get_core_engine)
):
    """Requests to pause a running task."""
    try:
        await coreEngine.pauseTask(taskId)
        task = await coreEngine.getTask(taskId)
        if not task:
             raise ValueError("Task disappeared after pausing.")
        return task
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task with ID '{taskId}' not found.")

@router.post(
    "/{taskId}/resume", 
    response_model=Task, 
    summary="Resume a Task"
)
async def resume_task(
    taskId: str,
    coreEngine: CoreEngine = Depends(get_core_engine)
):
    """Requests to resume a paused task."""
    try:
        await coreEngine.resumeTask(taskId)
        task = await coreEngine.getTask(taskId)
        if not task:
            raise ValueError("Task disappeared after resuming.")
        return task
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Task with ID '{taskId}' not found.")

@router.delete(
    "/{taskId}", 
    status_code=status.HTTP_204_NO_CONTENT, 
    summary="Cancel and Delete a Task"
)
async def cancel_and_delete_task(
    taskId: str,
    cleanup: bool = False, # 作为查询参数
    coreEngine: CoreEngine = Depends(get_core_engine)
):
    """
    Cancels a running task. The task record will be marked as failed.
    - `cleanup`: If true, associated workers will attempt to delete temporary files.
    """
    try:
        await coreEngine.cancelTask(taskId, cleanup=cleanup)
        # 成功后没有内容返回
    except ValueError as e:
        logger.warning(f"Attempted to cancel task '{taskId}', but failed: {e}")
        # 即使找不到任务，DELETE 操作也应该是幂等的，所以可以不抛出 404
        pass

@router.put(
    "/{taskId}/config", 
    status_code=status.HTTP_202_ACCEPTED,
    summary="Update a Running Task's Configuration"
)
async def update_task_configuration(
    taskId: str,
    request: UpdateTaskConfigRequest,
    configService: ConfigService = Depends(get_config_service)
):
    """
    Dynamically updates a configuration value for a specific running task.
    This change is persisted as a task-level override.
    """
    try:
        await configService.setTaskConfigOverride(taskId, request.key, request.value)
        return {"message": "Configuration update request accepted."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
