from __future__ import annotations

from uuid import UUID

from prefect import State, flow, get_run_logger, task
from prefect.artifacts import create_progress_artifact
from prefect.client.schemas.objects import FlowRun
from prefect.runtime import flow_run
from pydantic import ValidationError

from .config import (
    Config,
    Range,
    build_flow_run_name,
    coerce_config,
    get_total_target,
    resolve_range,
    validate_config as validate_config_data,
)

CHUNK_SIZE = 100


def run_scenario_flow_run_name() -> str:
    raw_config = flow_run.parameters.get("config")
    if raw_config is None:
        return "Run Scenario"

    try:
        config = coerce_config(raw_config)
    except ValidationError:
        return "Run Scenario"

    return build_flow_run_name(config)


def crash_handler(flow, flow_run: FlowRun, state: State):  # noqa: ARG001
    print(f"Flow {flow_run.name!r} crashed with state {state!r}")


@task
def validate_config(config: Config) -> None:
    validate_config_data(config)


@flow(
    name="Run Scenario",
    flow_run_name=run_scenario_flow_run_name,
    log_prints=True,
    on_crashed=[crash_handler],
)
async def run_scenario(config: Config):
    logger = get_run_logger()

    validate_config(config)
    total_target = get_total_target(config)

    total_artifact_id: UUID = create_progress_artifact(
        progress=0.0,
        key="total-progress",
        description=f"Total completed: 0 / {total_target:,}",
    )

    num_chunks = (total_target + CHUNK_SIZE - 1) // CHUNK_SIZE
    logger.info("Prepared %s chunk(s) for %s scenario(s).", num_chunks, total_target)

    if isinstance(config.sites, Range):
        start_index, end_index = resolve_range(config)
        logger.info("Range selection resolved to indices %s-%s.", start_index, end_index)
    else:
        logger.info("Site ID selection includes %s site(s).", total_target)

    logger.debug("Created total progress artifact %s.", total_artifact_id)
