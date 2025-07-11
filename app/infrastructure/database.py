import json
from typing import Any, Dict, List, Optional

import aiosqlite
from loguru import logger

from sdk.ghost_downloader_sdk.models import Task, TaskStage, TaskStatus, OverallTaskStatus, DisplayIntent


def encodeJson(data: Any) -> str:
    """Encodes Python objects (including Pydantic models) to a JSON string."""
    if hasattr(data, 'model_dump'):
        return data.model_dump_json()
    return json.dumps(data, ensure_ascii=False)

def decodeJson(data: str) -> Any:
    """Decodes a JSON string to Python objects."""
    return json.loads(data)


CREATE_TABLES_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tasks (
    taskId TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    overallStatus TEXT NOT NULL,
    currentStageId TEXT,
    createdAt INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_stages (
    stageId TEXT PRIMARY KEY,
    taskId TEXT NOT NULL,
    stageIndex INTEGER NOT NULL,
    displayIntent TEXT NOT NULL, -- 使用 TEXT 存储 JSON 字符串
    workerType TEXT NOT NULL,
    instructionPayload TEXT NOT NULL, -- 使用 TEXT
    status TEXT NOT NULL,
    progress REAL NOT NULL,
    FOREIGN KEY (taskId) REFERENCES tasks (taskId) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ownerId TEXT NOT NULL,
    metaKey TEXT NOT NULL,
    metaValue TEXT NOT NULL, -- 使用 TEXT
    UNIQUE (ownerId, metaKey)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL -- 使用 TEXT
);

CREATE INDEX IF NOT EXISTS idx_metadata_ownerId ON metadata (ownerId);
CREATE INDEX IF NOT EXISTS idx_task_stages_taskId ON task_stages (taskId);
"""

class Database:
    """
    A lightweight, high-performance database service using aiosqlite and msgspec.
    This class encapsulates all SQL operations and data mapping logic.
    """
    _dbPath: str
    _conn: aiosqlite.Connection

    def __init__(self, dbPath: str):
        self._dbPath = dbPath
        logger.info(f"Database service targeting file: {self._dbPath}")

    async def connect(self):
        """Establishes a connection and ensures tables are created."""
        self._conn = await aiosqlite.connect(self._dbPath)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(CREATE_TABLES_SQL)
        await self._conn.commit()
        logger.info("Database connection established and tables ensured.")

    async def close(self):
        """Closes the database connection."""
        await self._conn.close()
        logger.info("Database connection closed.")
        
    # --- 数据映射辅助方法 ---
    @staticmethod
    def _mapRowToTask(row: aiosqlite.Row) -> Task:
        """Maps a database row to a Pydantic Task model."""
        return Task.model_validate(dict(row))

    @staticmethod
    def _mapRowToTaskStage(row: aiosqlite.Row) -> TaskStage:
        """Maps a database row to a Pydantic TaskStage model."""
        data = dict(row)
        # 手动将 JSON 字符串解码回 Pydantic 模型或字典
        data['displayIntent'] = DisplayIntent.model_validate_json(data['displayIntent'])
        data['instructionPayload'] = decodeJson(data['instructionPayload'])
        return TaskStage.model_validate(data)

    # --- Task & Stage CRUD ---
    async def createTask(self, task: Task) -> None:
        sql = "INSERT INTO tasks (taskId, title, overallStatus, currentStageId, createdAt) VALUES (?, ?, ?, ?, ?)"
        params = (task.taskId, task.title, task.overallStatus.value, task.currentStageId, task.createdAt)
        await self._conn.execute(sql, params)
        await self._conn.commit()

    async def getTask(self, taskId: str) -> Optional[Task]:
        sql = "SELECT * FROM tasks WHERE taskId = ?"
        async with self._conn.execute(sql, (taskId,)) as cursor:
            row = await cursor.fetchone()
            return self._mapRowToTask(row) if row else None

    async def getAllTasks(self) -> List[Task]:
        """Retrieves all parent tasks from the database."""
        # 注意：这里只获取顶层的 Task，不包括它们的 stages 和 metadata
        sql = "SELECT * FROM tasks ORDER BY createdAt DESC"
        async with self._conn.execute(sql) as cursor:
            rows = await cursor.fetchall()
            return [self._mapRowToTask(row) for row in rows]

    async def getAllMetadata(self, ownerId: str) -> Dict[str, Any]:
        """Retrieves all metadata key-value pairs for a given owner ID."""
        sql = "SELECT metaKey, metaValue FROM metadata WHERE ownerId = ?"
        async with self._conn.execute(sql, (ownerId,)) as cursor:
            rows = await cursor.fetchall()
            return {row['metaKey']: decodeJson(row['metaValue']) for row in rows}
            
    async def getStagesForTask(self, taskId: str) -> List[TaskStage]:
        sql = "SELECT * FROM task_stages WHERE taskId = ? ORDER BY stageIndex"
        async with self._conn.execute(sql, (taskId,)) as cursor:
            rows = await cursor.fetchall()
            return [self._mapRowToTaskStage(row) for row in rows]

    async def addStages(self, stages: List[TaskStage]) -> None:
        sql = "INSERT INTO task_stages (stageId, taskId, stageIndex, displayIntent, workerType, instructionPayload, status, progress) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        stageTuples = [(s.stageId, s.taskId, s.stageIndex, encodeJson(s.displayIntent), s.workerType,
                        encodeJson(s.instructionPayload), s.status.value, s.progress) for s in stages]
        await self._conn.executemany(sql, stageTuples)
        await self._conn.commit()

    async def updateStage(self, stageId: str, updates: Dict) -> bool:
        setClause = ", ".join([f"{key} = ?" for key in updates.keys()])
        values = list(updates.values())
        sql = f"UPDATE task_stages SET {setClause} WHERE stageId = ?"
        values.append(stageId)
        cursor = await self._conn.execute(sql, tuple(values))
        await self._conn.commit()
        return cursor.rowcount > 0
    
    async def updateTaskStatus(self, taskId: str, status: OverallTaskStatus, currentStageId: Optional[str] = None):
        updates = {'overallStatus': status.value}
        if currentStageId is not None:
            updates['currentStageId'] = currentStageId
        setClause = ", ".join([f"{key} = ?" for key in updates.keys()])
        values = list(updates.values())
        sql = f"UPDATE tasks SET {setClause} WHERE taskId = ?"
        values.append(taskId)
        await self._conn.execute(sql, tuple(values))
        await self._conn.commit()

    # --- 高级查询方法 ---
    async def getTaskWithDetails(self, taskId: str) -> Optional[Dict[str, Any]]:
        """
        Fetches a parent task and eagerly loads all its associated stages and metadata.
        This method is designed to be a one-stop-shop for getting all information
        related to a single logical task.

        Returns:
            A dictionary representing the task and its nested details, or None if not found.
        """
        task = await self.getTask(taskId)
        if not task:
            return None

        taskDict = task.model_dump()

        stages = await self.getStagesForTask(taskId)
        taskDict['stages'] = [s.model_dump() for s in stages]

        taskDict['metadata'] = await self.getAllMetadata(taskId)
        return taskDict

    async def getStage(self, stageId: str) -> Optional[TaskStage]:
        """Retrieves a single stage by its primary key."""
        sql = "SELECT * FROM task_stages WHERE stageId = ?"
        async with self._conn.execute(sql, (stageId,)) as cursor:
            row = await cursor.fetchone()
            return self._mapRowToTaskStage(row) if row else None

    async def getActiveStagesForTask(self, taskId: str) -> List[TaskStage]:
        """
        Retrieves all stages for a task that are in a non-terminal state
        (i.e., WAITING or RUNNING).
        """
        active_statuses = (TaskStatus.WAITING.value, TaskStatus.RUNNING.value)
        # 使用参数化查询来安全地处理 IN 子句
        placeholders = ','.join(['?'] * len(active_statuses))
        sql = f"SELECT * FROM task_stages WHERE taskId = ? AND status IN ({placeholders})"

        async with self._conn.execute(sql, (taskId, *active_statuses)) as cursor:
            rows = await cursor.fetchall()
            return [self._mapRowToTaskStage(row) for row in rows]

    async def getMaxStageIndex(self, taskId: str) -> Optional[int]:
        """
        Finds the maximum stageIndex for a given task.
        Returns None if no stages exist for the task.
        """
        sql = "SELECT MAX(stageIndex) FROM task_stages WHERE taskId = ?"
        async with self._conn.execute(sql, (taskId,)) as cursor:
            row = await cursor.fetchone()
            # row[0] will be None if there are no rows, which is the correct behavior
            return row[0] if row else None

    async def getNextRunnableStages(self, taskId: str) -> List[TaskStage]:
        sql = "SELECT * FROM task_stages WHERE taskId = ? AND status = ? AND stageIndex = (SELECT MIN(stageIndex) FROM task_stages WHERE taskId = ? AND status != ?)"
        params = (taskId, TaskStatus.WAITING.value, taskId, TaskStatus.COMPLETED.value)
        async with self._conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            return [self._mapRowToTaskStage(row) for row in rows]
            
    async def hasPendingStages(self, taskId: str) -> bool:
        sql = "SELECT 1 FROM task_stages WHERE taskId = ? AND status != ? LIMIT 1"
        async with self._conn.execute(sql, (taskId, TaskStatus.COMPLETED.value)) as cursor:
            return await cursor.fetchone() is not None

    # --- Metadata & Settings Methods ---
    async def upsertMetadata(self, ownerId: str, key: str, value: Any) -> None:
        sql = "INSERT INTO metadata (ownerId, metaKey, metaValue) VALUES (?, ?, ?) ON CONFLICT(ownerId, metaKey) DO UPDATE SET metaValue = excluded.metaValue"
        await self._conn.execute(sql, (ownerId, key, encodeJson(value)))
        await self._conn.commit()
            
    async def getMetadataValue(self, ownerId: str, key: str) -> Optional[Any]:
        sql = "SELECT metaValue FROM metadata WHERE ownerId = ? AND metaKey = ?"
        async with self._conn.execute(sql, (ownerId, key)) as cursor:
            row = await cursor.fetchone()
            return decodeJson(row['metaValue']) if row and row['metaValue'] else None

    async def saveSettings(self, settings: Dict[str, Any]):
        """Saves or updates multiple settings in a single transaction."""
        sql = """
        INSERT INTO settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """
        params = [(key, encodeJson(value)) for key, value in settings.items()]
        async with self._conn.cursor() as cursor:
             await cursor.executemany(sql, params)
        await self._conn.commit()
        logger.info(f"Bulk saved {len(settings)} settings to database.")

    async def saveSetting(self, key: str, value: Any):
        sql = "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        await self._conn.execute(sql, (key, encodeJson(value)))
        await self._conn.commit()
        logger.info(f"Setting '{key}' saved to database.")
        
    async def loadAllSettings(self) -> Dict[str, Any]:
        sql = "SELECT key, value FROM settings"
        async with self._conn.execute(sql) as cursor:
            rows = await cursor.fetchall()
            return {row['key']: decodeJson(row['value']) for row in rows}
