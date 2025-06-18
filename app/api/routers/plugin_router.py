from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from typing import List, Dict, Any
from loguru import logger
from pydantic import BaseModel

from app.infrastructure.plugin.plugin_service import UnifiedPluginService
from app.api.deps import get_plugin_service

# --- 请求体模型  ---
class InstallPluginRequest(BaseModel):
    url: str

# --- API 路由 ---
router = APIRouter(prefix="/plugins", tags=["Plugins"])

@router.get(
    "/",
    response_model=List[Dict[str, str]],
    summary="List All Installed Plugins"
)
async def list_installed_plugins(
    pluginService: UnifiedPluginService = Depends(get_plugin_service)
):
    """
    Retrieves the metadata of all currently loaded feature packs.
    The GUI can use this to display a list of installed plugins.
    """
    loaded_packs = pluginService.getLoadedPacks()
    # 插件的 metadata 本身就是一个字典，可以直接返回，FastAPI 会处理好 JSON 序列化
    return [pack.metadata for pack in loaded_packs]

@router.post(
    "/reload",
    response_model=Dict[str, Any],
    summary="Trigger a Manual Reload of All Plugins"
)
async def reload_plugins(
    pluginService: UnifiedPluginService = Depends(get_plugin_service)
):
    """
    Manually triggers a full reload of all plugins from the 'features' directory.
    This is useful after manually adding or updating plugin files.
    """
    try:
        result = pluginService.triggerReload()
        logger.info("Plugin reload completed via API.")
        return result
    except Exception as e:
        logger.exception("An error occurred during manual plugin reload.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reload plugins: {e}"
        )

@router.post(
    "/upload",
    status_code=status.HTTP_201_CREATED,
    response_model=Dict[str, Any],
    summary="Install a New Plugin from a Local File"
)
async def install_plugin_from_upload(
    pluginService: UnifiedPluginService = Depends(get_plugin_service),
    file: UploadFile = File(...)
):
    """
    Installs a new plugin by uploading its .zip package file directly.
    This is the primary way to install plugins without relying on network downloads.
    """
    if not file.filename or not file.filename.endswith('.zip'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload a .zip package."
        )

    logger.info(f"Received plugin upload: '{file.filename}' of type '{file.content_type}'.")

    try:
        # installPluginFromFile 接收的是标准的文件流接口，完全兼容
        result_metadata = await pluginService.installPluginFromFile(file.filename, file.file)
        return {"message": "Plugin installed successfully.", "metadata": result_metadata}
    except ValueError as e: # 例如：插件已存在
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except Exception as e:
        logger.exception(f"Failed to install plugin from uploaded file '{file.filename}'.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not install plugin: {e}"
        )

@router.post(
    "/url/install",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Install a New Plugin from a URL"
)
async def install_plugin_from_url(
    request: InstallPluginRequest, # FastAPI 会自动用 Pydantic 解析请求体
    pluginService: UnifiedPluginService = Depends(get_plugin_service)
):
    """
    Initiates the installation of a new plugin from a given URL.
    This is an asynchronous process. The API returns immediately.
    """
    try:
        await pluginService.installPluginFromUrl(request.url)
        return {"message": "Plugin installation process started."}
    except NotImplementedError:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Installation from URL is not yet implemented.")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.exception(f"Failed to initiate plugin installation from URL '{request.url}'.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not start plugin installation: {e}"
        )


@router.delete(
    "/{pluginId}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Uninstall a Plugin"
)
async def uninstall_plugin(
    pluginId: str,
    pluginService: UnifiedPluginService = Depends(get_plugin_service)
):
    """
    Initiates the uninstallation of a plugin. This involves removing its
    files and then triggering a reload.
    """
    try:
        await pluginService.uninstallPlugin(pluginId)
        return {"message": f"Plugin '{pluginId}' uninstallation process started."}
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.exception(f"Failed to uninstall plugin '{pluginId}'.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not uninstall plugin: {e}"
        )
