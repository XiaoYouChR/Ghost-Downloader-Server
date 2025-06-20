from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from .models import CompletedTaskContext, ConfigField, WorkerCapabilities, StageDefinition


class IWorkerContext(ABC):
    """
    The context object provided to a Worker, allowing it to communicate
    back to the WorkerManager.
    """
    @abstractmethod
    async def reportProgress(self, progressData: Dict[str, Any]):
        """Reports the progress of the current task."""
        pass

    @abstractmethod
    async def reportCompletion(self, result: Dict[str, Any]):
        """Reports that the task has completed successfully."""
        pass

    @abstractmethod
    async def reportError(self, error: Exception):
        """Reports that the task has failed."""
        pass

    @property
    @abstractmethod
    def config(self) -> Dict[str, Any]:
        """Provides access to the effective configuration for this worker execution."""
        pass

    @abstractmethod
    def setCleanupFlag(self, cleanup: bool):
        self._cleanupOnCancel = cleanup

    @abstractmethod
    def shouldCleanupOnCancel(self) -> bool:
        return self._cleanupOnCancel

class IParser(ABC):
    @abstractmethod
    def canHandle(self, url: str) -> bool:
        """Returns True if this parser can handle the given URL."""
        pass

    @abstractmethod
    async def getInitialStages(self, url: str) -> List[StageDefinition]:
        """
        Parses a URL and returns a list of initial stages to be executed.
        This is the primary way a parser defines what needs to be done.
        """
        pass

class IWorkflow(ABC):
    @staticmethod
    def getCapabilities() -> WorkerCapabilities:
        """
        Returns a WorkerCapabilities object declaring the worker's features.
        """
        return WorkerCapabilities()

    @abstractmethod
    def canHandle(self, context: CompletedTaskContext) -> bool:
        """Returns True if this workflow should be run for the completed task."""
        pass

    @abstractmethod
    async def generatePlan(self, context: CompletedTaskContext) -> List[StageDefinition]:
        """Generates a plan of subsequent tasks to be executed."""
        pass

class IWorker(ABC):
    @abstractmethod
    async def execute(self, payload: Dict[str, Any], context: IWorkerContext):
        """The main execution method for a worker."""
        pass

    async def onConfigurationChanged(self, newConfig: Dict[str, Any]):
        """
        Called by the WorkerManager when a running task's configuration is updated.
        The worker should try to apply these changes dynamically if possible.
        """
        pass

    @staticmethod
    def getRequiredConfigKeys() -> List[str]:
        """Declares which config keys this worker actively uses."""
        return []

    @staticmethod
    def getPriority(taskPayload: Dict[str, Any]) -> int:
        """
        Returns a priority score for this worker to handle a given task payload.
        Higher numbers mean higher priority. The system will choose the worker
        with the highest priority. Default is 0.
        """
        return 0

class IFeaturePack(ABC):
    """The main interface for a feature pack plugin."""
    @property
    @abstractmethod
    def metadata(self) -> Dict[str, str]:
        """
        Returns metadata about the feature pack.
        e.g., {"id": "bilibili-pack", "name": "Bilibili Support", "version": "1.0.0"}
        """
        pass

    def getParsers(self) -> Optional[List[IParser]]:
        """Returns all parser implementations provided by this pack."""
        return None

    def getWorkflows(self) -> Optional[List[IWorkflow]]:
        """Returns all workflow implementations provided by this pack."""
        return None

    def registerWorkers(self) -> Dict[str, IWorker]:
        """
        Returns a dictionary mapping worker types to their implementations.
        e.g., { 'download:http': MyHttpWorker(), 'transcode:ffmpeg': MyFfmpegWorker() }
        """
        return {}

    def getConfigSchema(self) -> List[ConfigField]:
        """
        Returns a list of configuration fields this pack exposes.
        The key for each field should be unique, e.g., using a "packId.fieldName" convention.
        """
        return []
