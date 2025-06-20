import time
import uuid
from enum import Enum
from typing import List, Dict, Any, Optional, Literal

from pydantic import BaseModel, Field, model_validator


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


class DisplayIntent(BaseModel):
    """A language-agnostic, structured description of what a stage is doing."""

    key: str = Field(
        description="An i18n key for client-side localization, e.g., 'stage.description.merging_files'."
    )
    context: Dict[str, Any] = Field(
        default_factory=dict,
        description="Dynamic values for interpolating into the localized string.",
    )


class WorkerCapabilities(BaseModel):
    """A structured declaration of a worker's capabilities."""

    resumable: bool = Field(
        default=False, description="True if the worker supports pausing and resuming."
    )
    configurable: bool = Field(
        default=False,
        description="True if the worker's config can be updated mid-execution.",
    )


class ChoiceOption(BaseModel):
    """Represents a single option in a 'choice' type ConfigField."""

    label: str
    value: Any


class ConfigField(BaseModel):
    """A self-describing model for a single configuration field exposed by a feature pack."""

    key: str = Field(pattern=r"^[a-z0-9_]+\.[a-z0-9_.]+$")
    label: str
    fieldType: Literal["boolean", "string", "number", "choice", "secret"]
    defaultValue: Any
    description: str = ""
    choices: Optional[List[ChoiceOption]] = None
    group: Optional[str] = None
    placeholder: Optional[str] = None
    validationRules: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_consistency(self) -> "ConfigField":
        if self.fieldType == "choice":
            if not self.choices:
                raise ValueError(
                    "The 'choices' field is required when fieldType is 'choice'."
                )
            valid_values = [option.value for option in self.choices]
            if self.defaultValue not in valid_values:
                raise ValueError(
                    f"defaultValue '{self.defaultValue}' is not a valid choice."
                )
        return self


# --- Task & Stage Models: The heart of the persistence layer ---


class TaskStage(BaseModel):
    """Represents a single, executable stage within a parent Task."""

    taskId: str
    stageIndex: int
    displayIntent: DisplayIntent
    workerType: str
    stageId: str = Field(default_factory=lambda: f"stg_{uuid.uuid4().hex}")
    instructionPayload: Dict[str, Any] = Field(default_factory=dict)
    status: TaskStatus = TaskStatus.WAITING
    progress: float = Field(default=0.0, ge=0, le=1)


class Task(BaseModel):
    """Represents a logical, user-facing task, which is a collection of stages."""

    title: str
    taskId: str = Field(default_factory=lambda: f"tsk_{uuid.uuid4().hex}")
    overallStatus: OverallTaskStatus = OverallTaskStatus.RUNNING
    currentStageId: Optional[str] = None
    createdAt: int = Field(default_factory=lambda: int(time.time()))
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="A flexible dictionary for storing task-level context, "
        "such as source URL, config overrides, or plugin-specific data.",
    )


# --- Plugin Interaction Models: Data flowing between components ---


class CompletedTaskContext(BaseModel):
    """
    Context provided to a Workflow plugin, describing the parent task
    and all its completed stages, enabling intelligent decision-making.
    """

    parentTask: Task
    completedStages: List[TaskStage]


class StageDefinition(BaseModel):
    """
    A blueprint for creating a new TaskStage, returned by a Parser or a Workflow.
    This is an immutable "recipe" for the CoreEngine to follow.
    """

    displayIntent: DisplayIntent
    workerType: str
    payload: Dict[str, Any] = Field(default_factory=dict)
