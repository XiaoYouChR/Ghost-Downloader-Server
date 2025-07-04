from typing import List, Dict, Optional

from loguru import logger

from sdk.ghost_downloader_sdk.interfaces import IFeaturePack, IParser, IWorkflow, IWorker
from sdk.ghost_downloader_sdk.models import ConfigField

from .parser import HttpParser
from .worker import HttpDownloadWorker


class HttpFeaturePack(IFeaturePack):
    """
    Provides the core capability to download files over HTTP/HTTPS.
    This pack includes a generic URL parser and a robust download worker.
    """
    def __init__(self):
        logger.info("HttpFeaturePack instance created.")
        self._parser = HttpParser()
        self._worker = HttpDownloadWorker()

    @property
    def metadata(self) -> Dict[str, str]:
        return {
            "id": "core.http_downloader",
            "name": "HTTP/S Downloader",
            "version": "1.0.0",
            "author": "XiaoYouChR"
        }

    def getParsers(self) -> List[IParser]:
        """Returns the generic HTTP parser instance."""
        return [self._parser]

    def registerWorkers(self) -> Dict[str, IWorker]:
        """Registers the http download worker instance."""
        return {
            "download:http": self._worker
        }

    def getConfigSchema(self) -> List[ConfigField]:
        # return [
        #     ConfigField(key="core.http_downloader.timeout", ...),
        #     ConfigField(key="core.http_downloader.user_agent", ...),
        # ]
        return []
