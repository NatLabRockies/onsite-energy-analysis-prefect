from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .config import SizingStrategy, Technology


class SimulationJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    site_id: str
    technology: Technology
    sizing_strategy: SizingStrategy
    overwrite_existing_results: bool = True
    working_directory: str
    command: list[str]
    result_prefix: str
    exact_result_keys: list[str] = Field(default_factory=list)
    environment: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SimulationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    site_id: str
    status: Literal["completed", "skipped", "failed"]
    return_code: int | None
    host: str
    duration_seconds: float | None
    uploaded_count: int = 0
