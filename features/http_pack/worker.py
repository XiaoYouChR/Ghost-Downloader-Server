import asyncio
from typing import Dict, Any, List
from loguru import logger

from sdk.ghost_downloader_sdk.interfaces import IWorker, IWorkerContext
from sdk.ghost_downloader_sdk.models import WorkerCapabilities

class HttpDownloadWorker(IWorker):
    """
    A core worker responsible for downloading files over HTTP/HTTPS.
    It supports resumable downloads.
    """
    @staticmethod
    def getCapabilities() -> WorkerCapabilities:
        """Declare that this worker is resumable."""
        return WorkerCapabilities(resumable=True, configurable=True)

    @staticmethod
    def getRequiredConfigKeys() -> List[str]:
        """Declare the configuration keys this worker depends on."""
        return [
            "core.http_downloader.timeout",
            "core.http_downloader.user_agent",
            "server.proxy_url" # 也可以依赖服务器的全局配置
        ]

    @staticmethod
    def getPriority(taskPayload: Dict[str, Any]) -> int:
        """
        This is the generic, fallback downloader. It should always have
        a low, but non-zero, priority.
        """
        return 1 # 优先级为 1，高于默认的 0

    async def execute(self, payload: Dict[str, Any], context: IWorkerContext):
        """
        The main execution logic for downloading a file.
        This is a placeholder implementation focusing on the interaction pattern.
        """
        url = payload.get('url')
        savePath = payload.get('savePath') # 这个路径应该由 CoreEngine 填充

        logger.info(f"[{context.stageId}] Starting HTTP download from: {url}")

        # 从上下文中获取最终生效的配置
        timeout = context.config.get('core.http_downloader.timeout', 30)
        userAgent = context.config.get('core.http_downloader.user_agent', 'GhostDownloader/1.0')
        proxy = context.config.get('server.proxy_url')

        logger.debug(f"[{context.stageId}] Config: timeout={timeout}, user-agent='{userAgent}', proxy='{proxy}'")

        # --- 这里是伪代码，代表真实的下载逻辑 ---
        try:
            total_size = 1000 # 模拟获取文件大小
            downloaded = 0

            # 模拟下载循环
            while downloaded < total_size:
                # 检查是否有取消请求
                await asyncio.sleep(0.5) # 模拟下载一小块数据
                downloaded += 100

                # 报告进度
                progress = downloaded / total_size
                await context.reportProgress(progress)

                # 定期保存断点信息
                resume_data = {"downloaded_bytes": downloaded, "total_size": total_size}
                await context.saveResumeData(resume_data)

            logger.success(f"[{context.stageId}] Download finished for: {url}")
            # 报告完成
            await context.reportCompletion({"filePath": savePath})

        except asyncio.CancelledError:
            # 在这里处理取消逻辑
            logger.warning(f"[{context.stageId}] Download was cancelled.")
            # 保存最后一次的断点信息
            await context.saveResumeData({"downloaded_bytes": downloaded, "total_size": total_size})
            # 无需再向上抛出，_executeWrapper 会处理
            raise # 重新抛出，让 WorkerManager 知道任务被取消了

        except Exception as e:
            logger.error(f"[{context.stageId}] An error occurred during download: {e}")
            await context.reportError(e)
