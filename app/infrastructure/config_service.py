from collections.abc import Callable
from typing import Any, Dict, List, Optional, Awaitable

from loguru import logger

from sdk.ghost_downloader_sdk.models import ConfigField, Task
from .database import Database
from .plugin.plugin_service import UnifiedPluginService


class ConfigService:
    """
    Manages all configurations for the application and its plugins.
    It provides a hierarchical configuration system:
    Task Override > Global User Setting > Plugin Default.
    """

    def __init__(self, db: Database, pluginService: UnifiedPluginService):
        self._db = db
        self._pluginService = pluginService

        # 缓存
        self._globalSettings: Dict[str, Any] = {}
        self._schemaRegistry: Dict[str, ConfigField] = {}

        self.onTaskConfigChanged: Callable[[str, Dict[str, Any]], Awaitable] | None = None

    # --- 生命周期方法 ---

    async def initialize(self):
        """
        Loads all settings and schemas. Should be called at application startup
        after plugins have been loaded.
        """
        logger.info("Initializing ConfigService...")
        await self._loadGlobalSettingsFromDb()
        self._buildSchemaRegistry()
        logger.info(
            f"ConfigService initialized with {len(self._schemaRegistry)} known config keys."
        )

    # --- 核心查询逻辑 ---

    def getEffectiveConfig(self, key: str, task: Optional[Task] = None) -> Any:
        """
        Gets the final, effective value for a config key by checking all layers.
        """
        # 检查任务级覆盖 (Task-level override)
        if task and task.metadata:
            configOverrides = task.metadata.get("configOverrides", {})
            if key in configOverrides:
                logger.debug(
                    f"Config key '{key}' found in task override. Value: {configOverrides[key]}"
                )
                return configOverrides[key]

        # 检查全局用户设置 (Global user setting)
        if key in self._globalSettings:
            logger.debug(
                f"Config key '{key}' found in global settings. Value: {self._globalSettings[key]}"
            )
            return self._globalSettings[key]

        # 回退到插件或服务器定义的默认值 (Plugin/Server default)
        if key in self._schemaRegistry:
            defaultValue = self._schemaRegistry[key].defaultValue
            logger.debug(
                f"Config key '{key}' found in schema. Using default value: {defaultValue}"
            )
            return defaultValue

        # 最终回退
        logger.warning(f"Config key '{key}' not found in any layer. Returning None.")
        return None

    # --- 全局设置管理 ---

    async def setGlobalSetting(self, key: str, value: Any):
        """Updates a global setting and persists it to the database."""
        # TODO: Add validation against the schema before saving.
        if key not in self._schemaRegistry:
            raise ValueError(f"Configuration key '{key}' is not defined in any schema.")

        logger.info(f"Updating global setting: '{key}' = {value}")
        self._globalSettings[key] = value
        await self._db.saveSetting(key, value)

    async def setGlobalSettings(self, settings: Dict[str, Any]):
        """
        Atomically updates multiple global settings.
        This is more efficient than calling setGlobalSetting in a loop.
        """
        # 1. 验证所有 key
        for key in settings.keys():
            if key not in self._schemaRegistry:
                raise ValueError(
                    f"Attempted to set an unknown configuration key: '{key}'"
                )

        logger.info(f"Updating {len(settings)} global setting(s).")

        # 2. 在内存中更新缓存
        self._globalSettings.update(settings)

        # 3. 批量更新数据库
        await self._db.saveSettings(settings)

    def getGlobalSetting(self, key: str) -> Any:
        """Gets a global setting, falling back to its default value."""
        return self._globalSettings.get(
            key,
            self._schemaRegistry.get(
                key,
                ConfigField(key="", label="", fieldType="string", defaultValue=None),
            ).defaultValue,
        )

    def getGlobalSettings(self) -> Dict[str, Any]:
        """
        Public getter for the currently set global configurations.
        Returns a copy to prevent external mutation.
        """
        return self._globalSettings.copy()

    # --- 单个任务配置的更新 ---

    async def setTaskConfigOverride(self, taskId: str, configKey: str, value: Any):
        """
        Sets or updates a configuration override for a specific task.
        This change is persisted and a notification is triggered for real-time updates.
        """
        logger.info(
            f"Setting config override for Task '{taskId}': '{configKey}' = {value}"
        )

        # 验证 Key 的合法性
        if configKey not in self._schemaRegistry:
            raise ValueError(
                f"Attempted to set an unknown configuration key: '{configKey}'"
            )

        # 从数据库获取当前的覆盖配置
        currentOverrides = (
            await self._db.getMetadataValue(ownerId=taskId, key="config:overrides")
            or {}
        )

        currentOverrides[configKey] = value

        # 将更新后的完整覆盖配置持久化回数据库
        await self._db.upsertMetadata(
            ownerId=taskId, key="config:overrides", value=currentOverrides
        )

        # 通知其他服务这个任务的配置发生了变化
        if self.onTaskConfigChanged:
            # 只通知被更改的那个键值对
            await self.onTaskConfigChanged(taskId, {configKey: value})
        else:
            logger.warning(
                "onTaskConfigChanged callback is not set. Real-time update will not be triggered."
            )

    async def getTaskConfigOverrides(self, taskId: str) -> Dict[str, Any]:
        """Retrieves all configuration overrides for a specific task."""
        overrides = await self._db.getMetadataValue(
            ownerId=taskId, key="config:overrides"
        )
        return overrides or {}

    # --- Schema 管理 ---

    def getAllConfigSchemas(self) -> List[ConfigField]:
        """Aggregates schemas from all loaded plugins for the GUI."""
        return list(self._schemaRegistry.values())

    # --- 内部辅助方法 ---

    async def _loadGlobalSettingsFromDb(self):
        """Loads all settings from the database into the memory cache."""
        self._globalSettings = await self._db.loadAllSettings()
        logger.info(
            f"Loaded {len(self._globalSettings)} global settings from database."
        )

    def _buildSchemaRegistry(self):
        """Builds a flat dictionary of all known config keys from all plugins."""
        registry: Dict[str, ConfigField] = {}

        # TODO: Define server configs in a structured way

        server_configs = [
            ConfigField(
                key="server.max_concurrent_workers",
                label="Max Concurrent Workers",
                fieldType="number",
                defaultValue=4,
            ),
            ConfigField(
                key="server.default_save_path",
                label="Default Save Path",
                fieldType="string",
                defaultValue="/downloads",
            ),
        ]
        for config_field in server_configs:
            registry[config_field.key] = config_field

        # 从所有插件加载配置
        for pack in self._pluginService._featurePacks.values():
            for config_field in pack.getConfigSchema():
                if config_field.key in registry:
                    logger.warning(
                        f"Duplicate config key '{config_field.key}' detected. Overwriting."
                    )
                registry[config_field.key] = config_field

        self._schemaRegistry = registry
