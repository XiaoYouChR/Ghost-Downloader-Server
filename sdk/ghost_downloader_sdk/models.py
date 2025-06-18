import time
import uuid
from enum import Enum
from typing import List, Dict, Any, Optional, Literal
import msgspec

# --- Enums ---

class TaskStatus(str, Enum):
    """
    Enumeration for the lifecycle status of an individual TaskStage.
    """
    WAITING = "waiting"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"

class OverallTaskStatus(str, Enum):
    """
    High-level status for a parent Task, primarily for UI display.
    """
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


# --- Core Data Structures  ---

class DisplayIntent(msgspec.Struct, frozen=True):
    """A language-agnostic, structured description of what a stage is doing."""
    key: str
    context: Dict[str, Any] = msgspec.field(default_factory=dict)

class WorkerCapabilities(msgspec.Struct, frozen=True):
    """A structured declaration of a worker's capabilities."""
    resumable: bool = False
    configurable: bool = False

class ChoiceOption(msgspec.Struct, frozen=True):
    """Represents a single option in a 'choice' type ConfigField."""
    label: str
    value: Any

class ConfigField(msgspec.Struct, frozen=True):
    """A self-describing model for a single configuration field exposed by a feature pack."""
    key: str
    label: str
    fieldType: Literal["boolean", "string", "number", "choice", "secret"]
    defaultValue: Any

    description: str = ""
    choices: Optional[List[ChoiceOption]] = None
    group: Optional[str] = None
    placeholder: Optional[str] = None
    validationRules: Optional[Dict[str, Any]] = msgspec.field(default_factory=dict)


# --- Task & Stage Models: The heart of the persistence layer ---

class TaskStage(msgspec.Struct):
    """Represents a single, executable stage within a parent Task."""
    taskId: str
    stageIndex: int
    displayIntent: DisplayIntent
    workerType: str

    stageId: str = msgspec.field(default_factory=lambda: f"stg_{uuid.uuid4().hex}")
    instructionPayload: Dict[str, Any] = msgspec.field(default_factory=dict)
    status: TaskStatus = TaskStatus.WAITING
    progress: float = 0.0

class Task(msgspec.Struct):
    """Represents a logical, user-facing task, which is a collection of stages."""
    # 必需字段在前
    title: str
    # 可选/有默认值的字段在后
    taskId: str = msgspec.field(default_factory=lambda: f"tsk_{uuid.uuid4().hex}")
    overallStatus: OverallTaskStatus = OverallTaskStatus.RUNNING
    currentStageId: Optional[str] = None
    createdAt: int = msgspec.field(default_factory=lambda: int(time.time()))


# --- Plugin Interaction Models: Data flowing between components ---

class CompletedTaskContext(msgspec.Struct):
    """
    Context provided to a Workflow plugin, describing the parent task
    and all its completed stages, enabling intelligent decision-making.
    """
    parentTask: Task
    completedStages: List[TaskStage]

class StageDefinition(msgspec.Struct, frozen=True):
    """
    A blueprint for creating a new TaskStage, returned by a Parser or a Workflow.
    This is an immutable "recipe" for the CoreEngine to follow.
    """
    displayIntent: DisplayIntent
    workerType: str
    payload: Dict[str, Any] = msgspec.field(default_factory=dict)