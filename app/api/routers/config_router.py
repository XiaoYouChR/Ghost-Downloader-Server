from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any
from loguru import logger
from pydantic import BaseModel

from app.infrastructure.config_service import ConfigService
from app.api.deps import get_config_service

from sdk.ghost_downloader_sdk.models import ConfigField

# --- 请求体模型 ---
class UpdateGlobalConfigRequest(BaseModel):
    settings: Dict[str, Any]

# --- API 路由 ---
router = APIRouter(prefix="/config", tags=["Configuration"])

@router.get(
    "/schema",
    response_model=List[ConfigField],
    summary="Get All Configuration Schemas"
)
async def get_all_configuration_schemas(
    configService: ConfigService = Depends(get_config_service)
):
    """
    Retrieves a complete list of all configuration field definitions
    from the server and all loaded plugins. The GUI uses this to dynamically
    render the entire settings page.
    """
    return configService.getAllConfigSchemas()

@router.get(
    "/values",
    response_model=Dict[str, Any],
    summary="Get All Global Configuration Values"
)
async def get_all_global_configuration_values(
    configService: ConfigService = Depends(get_config_service)
):
    """
    Retrieves the current values of all globally configured settings.
    This does NOT include plugin defaults unless they have been explicitly set by the user.
    """
    return configService.getGlobalSettings()

@router.put(
    "/values",
    response_model=Dict[str, Any],
    summary="Update Global Configuration Values"
)
async def update_global_configuration_values(
    request: UpdateGlobalConfigRequest,
    configService: ConfigService = Depends(get_config_service)
):
    """
    Updates one or more global configuration values.
    The request body should be a dictionary of key-value pairs.
    """
    try:
        # 批量更新
        await configService.setGlobalSettings(request.settings)
        # 返回更新后的所有全局设置
        return configService.getGlobalSettings()
    except ValueError as e: # 通常是 key 不存在
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception:
        logger.exception("Unexpected error updating global settings.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update settings.")

# 为单个任务更新配置的端点放在 task_router 中更符合 RESTful 风格
# 这里不再重复实现 PUT /tasks/{taskId}/config