from weakref import ref, WeakKeyDictionary
from typing import List, Dict, Optional, Any
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
        self.featureDir = featureDir

        # 能力注册表 (Ability Registries)
        self._parsers: List[RegisteredAbility] = []
        self._workflows: List[RegisteredAbility] = []
        self._workers: Dict[str, List[RegisteredAbility]] = {} # Key: workerType
        self._featurePacks: Dict[str, IFeaturePack] = {} # Key: packId

        # 一个用于反向查找的弱引用映射
        self._abilityToPackIdMap = WeakKeyDictionary()

    # --- 生命周期与重载控制 ---

    def loadPlugins(self):
        """
        Loads all feature pack classes, instantiates them, and registers their capabilities.
        """
        logger.info("Loading feature packs...")
        self._clearRegistries()

        loadedPackClasses = loadFeaturePackClassesFromDirectory(self.featureDir)

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

    def triggerReload(self):
        """
        Manually triggers a full reload of all plugins.
        This is the sole entry point for updating plugins during runtime.
        """
        logger.info("Manual plugin reload triggered.")
        self.loadPlugins()
        return {"status": "success", "loaded_packs": len(self._featurePacks)}

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

        # 1. 处理 preferredPackId 的情况 (通常用于内部或特定流程)
        if preferredPackId:
            for workerAbility in registeredWorkers:
                if workerAbility.packId == preferredPackId:
                    logger.debug(f"Found preferred worker for type '{workerType}' from pack '{preferredPackId}'.")
                    return workerAbility.implementation

        # 2 & 3. 基于优先级的选择
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
