import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from weakref import ref, WeakKeyDictionary
from typing import List, Dict, Optional, Any, IO
from zipfile import ZipFile

from loguru import logger


from sdk.ghost_downloader_sdk.interfaces import IFeaturePack, IParser, IWorkflow, IWorker
from .loader import loadFeaturePackClassesFromDirectory

class RegisteredAbility:
    """A wrapper that stores an ability and a reference to its parent pack."""
    def __init__(self, pack: IFeaturePack, implementation: IParser | IWorkflow | IWorker):
        # 使用弱引用，如果父包被卸载，这里的引用不会阻止其被垃圾回收
        self.packRef = ref(pack)
        self.implementation = implementation

    @property
    def packId(self) -> Optional[str]:
        pack = self.packRef()
        return pack.metadata.get('id') if pack else None

class UnifiedPluginService:
    """
    Manages the lifecycle and capabilities of all feature packs.
    Reloading is triggered manually to ensure stability.
    """
    def __init__(self, featureDir: str):
        self.featureDir = Path(featureDir).resolve()
        self.featureDir.mkdir(parents=True, exist_ok=True)

        # 能力注册表 (Ability Registries)
        self._parsers: List[RegisteredAbility] = []
        self._workflows: List[RegisteredAbility] = []
        self._workers: Dict[str, List[RegisteredAbility]] = {} # Key: workerType
        self._featurePacks: Dict[str, IFeaturePack] = {} # Key: packId

        # 用于反向查找的弱引用映射
        self._abilityToPackIdMap = WeakKeyDictionary()

    # --- 生命周期与重载控制 ---

    def loadPlugins(self):
        """
        Loads all feature pack classes, instantiates them, and registers their capabilities.
        """
        logger.info("Loading feature packs...")
        self._clearRegistries()

        loadedPackClasses = loadFeaturePackClassesFromDirectory(str(self.featureDir))

        for moduleName, packClass in loadedPackClasses:
            try:
                packInstance = packClass()
            except Exception as e:
                logger.error(f"Failed to instantiate feature pack class '{packClass.__name__}' from module '{moduleName}': {e}", exc_info=True)
                continue

            packId = packInstance.metadata.get('id')
            if not packId:
                logger.warning(f"Feature pack '{packInstance.metadata.get('name')}' is missing 'id'. Skipping.")
                continue

            if packId in self._featurePacks:
                logger.warning(f"Duplicate pack ID '{packId}'. Overwriting.")

            self._featurePacks[packId] = packInstance
            self._registerPackCapabilities(packInstance)

        self._logRegistryState()

    def triggerReload(self) -> Dict[str, Any]:
        """
        Manually triggers a full reload of all plugins.
        This is the sole entry point for updating plugins during runtime.
        """
        logger.info("Manual plugin reload triggered.")
        self.loadPlugins()
        return {
            "status": "success",
            "loadedPacks": len(self._featurePacks),
            "packIds": list(self._featurePacks.keys())
        }

    # --- 安装与卸载 ---
    async def installPluginFromFile(self, filename: str, fileStream: IO[bytes]) -> Dict[str, Any]:
        """
        Installs a plugin from a file stream (e.g., an upload).
        It extracts the zip to a temp location, validates it, moves it, and reloads.
        """
        logger.info(f"Starting installation of plugin from file: '{filename}'")

        with TemporaryDirectory() as tempDirStr:
            tempDir = Path(tempDirStr)
            tempZipPath = tempDir / filename

            with open(tempZipPath, 'wb') as f:
                shutil.copyfileobj(fileStream, f)

            # 解压并验证包结构
            extractedDir = tempDir / "extracted"
            with ZipFile(tempZipPath, 'r') as zipRef:
                zipRef.extractall(extractedDir)

            # 找到并验证插件的根目录
            # 我们期望 zip 包内只有一个根目录，这个目录就是插件本身
            extractedItems = list(extractedDir.iterdir())
            if len(extractedItems) != 1 or not extractedItems[0].is_dir():
                raise ValueError("ZIP archive must contain a single root directory for the plugin.")

            pluginSourceDir = extractedItems[0]

            # 验证 __init__.py 的存在
            if not (pluginSourceDir / '__init__.py').is_file():
                raise ValueError("Plugin directory must contain an '__init__.py' file.")

            # 插件的 ID 就是其根目录的名称
            pluginId = pluginSourceDir.name

            # 移动到最终的 features 目录
            targetDir = self.featureDir / pluginId
            if targetDir.exists():
                raise ValueError(f"Conflict: A plugin with ID '{pluginId}' already exists.")

            shutil.move(str(pluginSourceDir), str(targetDir))
            logger.info(f"Plugin '{pluginId}' successfully moved to '{targetDir}'.")

        # 触发重载以激活新插件
        self.triggerReload()

        # 确认插件已加载并返回其元数据
        newlyLoadedPack = self._featurePacks.get(pluginId)
        if not newlyLoadedPack:
            # 如果重载后没找到，说明插件本身可能有问题
            raise RuntimeError(f"Plugin '{pluginId}' was installed but failed to load. Please check server logs.")

        logger.success(f"Plugin '{pluginId}' installed and activated.")
        return newlyLoadedPack.metadata

    async def installPluginFromUrl(self, url: str):
        """
        (Placeholder) Initiates the installation of a new plugin from a URL.
        """
        logger.info(f"Plugin installation from URL requested: {url}. This feature is not yet implemented.")
        # TODO:
        # 1. Implement a basic, secure HTTP downloader here.
        # 2. Download the zip file to a temporary location.
        # 3. Call `installPluginFromFile` with the downloaded file stream.
        raise NotImplementedError("Plugin installation from URL is not yet implemented.")

    async def uninstallPlugin(self, pluginId: str):
        """Finds a plugin by its ID, removes its directory, and triggers a reload."""
        logger.info(f"Requesting to uninstall plugin with ID: '{pluginId}'")

        # 验证插件当前是否已加载
        if pluginId not in self._featurePacks:
            raise FileNotFoundError(f"Plugin with ID '{pluginId}' is not currently loaded or does not exist.")

        # 插件目录名约定与插件 ID 一致
        pluginDir = self.featureDir / pluginId

        if not pluginDir.is_dir():
             raise FileNotFoundError(f"Plugin directory for ID '{pluginId}' not found at expected location: '{pluginDir}'.")

        # 使用 shutil.rmtree 来递归删除目录，需要小心处理错误
        try:
            shutil.rmtree(pluginDir)
            logger.info(f"Removed plugin directory: '{pluginDir}'")
        except OSError as e:
            logger.error(f"Failed to remove plugin directory '{pluginDir}': {e}")
            raise IOError(f"Could not delete plugin files. Please check file permissions.")

        # 触发重载以移除已卸载的插件
        self.triggerReload()

        # 确认插件已被移除
        if pluginId in self._featurePacks:
            logger.error(f"Plugin '{pluginId}' was deleted but still loaded after reload. Check for caching or loading issues.")
            raise RuntimeError(f"Failed to fully unload plugin '{pluginId}'.")

        logger.success(f"Plugin '{pluginId}' uninstalled and unloaded successfully.")

    # --- 内部注册逻辑 ---

    def _clearRegistries(self):
        """Clears all registries before reloading."""
        self._parsers.clear()
        self._workflows.clear()
        self._workers.clear()
        self._featurePacks.clear()
        logger.debug("All plugin registries have been cleared.")

    def _registerPackCapabilities(self, pack: IFeaturePack):
        packId = pack.metadata['id']

        # 注册并建立反向映射
        for parser in pack.getParsers():
            registeredAbility = RegisteredAbility(pack, parser)
            self._parsers.append(registeredAbility)
            self._abilityToPackIdMap[parser] = packId

        for workflow in pack.getWorkflows():
            registeredAbility = RegisteredAbility(pack, workflow)
            self._workflows.append(registeredAbility)
            self._abilityToPackIdMap[workflow] = packId

        for workerType, workerImpl in pack.registerWorkers().items():
            registeredAbility = RegisteredAbility(pack, workerImpl)
            self._workers.setdefault(workerType, []).append(registeredAbility)
            self._abilityToPackIdMap[workerImpl] = packId

    def _logRegistryState(self):
        """Logs the current state of the registries."""
        logger.info(f"Total {len(self._featurePacks)} feature packs loaded.")
        logger.info(f"Registered Parsers: {len(self._parsers)}")
        logger.info(f"Registered Workflows: {len(self._workflows)}")
        logger.info(f"Registered Worker types: {list(self._workers.keys())}")

    # --- 公共查询 API ---

    def findParserForUrl(self, url: str) -> Optional[IParser]:
        """Finds the first parser that can handle the given URL."""
        for registeredParser in self._parsers:
            if registeredParser.implementation.canHandle(url):
                return registeredParser.implementation
        return None

    def findWorkflowsForTask(self, context) -> List[IWorkflow]:
        """Finds all workflows that can handle the completed task context."""
        return [
            rw.implementation for rw in self._workflows
            if rw.implementation.canHandle(context)
        ]

    def findBestWorker(self, workerType: str, taskPayload: Dict[str, Any], preferredPackId: Optional[str] = None) -> Optional[IWorker]:
        """
        Finds the best worker for a given type and payload by checking priorities.

        The selection logic is as follows:
        1. If a preferredPackId is provided, try to find a worker from that pack first.
        2. Otherwise, find all workers of the given type.
        3. Ask each worker for its priority score for the given task payload.
        4. Return the worker with the highest priority score.
        """
        registeredWorkers = self._workers.get(workerType, [])
        if not registeredWorkers:
            logger.warning(f"No workers found for type '{workerType}'.")
            return None

        # 1. 处理 preferredPackId 的情况
        if preferredPackId:
            for workerAbility in registeredWorkers:
                if workerAbility.packId == preferredPackId:
                    logger.debug(f"Found preferred worker for type '{workerType}' from pack '{preferredPackId}'.")
                    return workerAbility.implementation

        # 基于优先级的选择
        bestWorker: Optional[IWorker] = None
        highestPriority = -1

        for workerAbility in registeredWorkers:
            try:
                # 获取 Worker 类的静态方法 getPriority
                workerClass = type(workerAbility.implementation)
                priority = workerClass.getPriority(taskPayload)

                if priority > highestPriority:
                    highestPriority = priority
                    bestWorker = workerAbility.implementation

            except Exception as e:
                logger.error(f"Failed to get priority from worker '{type(workerAbility.implementation).__name__}': {e}")
                continue

        if bestWorker:
            logger.info(f"Selected best worker '{type(bestWorker).__name__}' for type '{workerType}' with priority {highestPriority}.")
        else:
            logger.warning(f"Could not select any worker for type '{workerType}' after checking priorities.")

        return bestWorker

    def findConfigKeysForWorker(self, workerType: str, taskPayload: Dict[str, Any]) -> List[str]:
        """

        Finds all configuration keys required by the best worker for a given task.
        """
        keys = set()

        bestWorker = self.findBestWorker(workerType, taskPayload)

        if not bestWorker:
            return []

        try:
            # 直接调用 worker 类的静态方法
            workerClass = type(bestWorker)
            requiredKeys = workerClass.getRequiredConfigKeys()
            keys.update(requiredKeys)
        except Exception as e:
            logger.error(f"Failed to get required config keys from worker '{type(bestWorker).__name__}': {e}")

        logger.debug(f"Found {len(keys)} required config keys for worker type '{workerType}': {keys}")
        return list(keys)

    # --- 辅助方法 ---

    def findPackIdForAbility(self, abilityInstance: Any) -> Optional[str]:
        """
        Efficiently finds the pack ID for a given ability instance using a weak map.
        """
        return self._abilityToPackIdMap.get(abilityInstance)

    def getLoadedPacks(self) -> List[IFeaturePack]:
        """Returns a list of all currently loaded feature pack instances."""
        return list(self._featurePacks.values())
