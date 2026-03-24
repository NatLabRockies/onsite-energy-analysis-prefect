from collections import Counter
from itertools import islice
from typing import Any, Iterable, Iterator

from prefect import State, flow, get_run_logger, task
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.futures import wait
from prefect.runtime import flow_run
from pydantic import ValidationError

from .command_builder import build_simulation_jobs
from .config import Config, build_flow_run_name, coerce_config, validate_config as validate_config_data, \
    get_requested_match_ids
from .jobs import SimulationJob
from .minio import filter_existing_jobs
from . import task_storage  # noqa: F401
from .tasks import run_simulation

DISPATCH_BATCH_SIZE = 100
TASK_MONITOR_TIMEOUT_SECONDS = 15


def dispatch_simulations_flow_run_name() -> str:
    raw_config = flow_run.parameters.get("config")
    if raw_config is None:
        return "Dispatch Simulations"

    try:
        config = coerce_config(raw_config)
    except ValidationError:
        return "Dispatch Simulations"

    return build_flow_run_name(config)


def crash_handler(flow, flow_run: FlowRun, state: State):  # noqa: ARG001
    print(f"Flow {flow_run.name!r} crashed with state {state!r}")


@task(name="Validate config", task_run_name="Validate config")
def validate_config(config: Config) -> None:
    validate_config_data(config)


@flow(
    name="dispatch-simulations",
    flow_run_name=dispatch_simulations_flow_run_name,
    log_prints=True,
    on_crashed=[crash_handler],
)
def dispatch_simulations(config: Config) -> dict[str, Any]:
    logger = get_run_logger()

    validate_config(config)
    site_ids = get_requested_match_ids(config)
    candidate_jobs = build_simulation_jobs(config, site_ids)
    queued_jobs, skipped_jobs = filter_existing_jobs(candidate_jobs)

    queued_futures = []
    for batch_number, job_batch in enumerate(_batched(queued_jobs, DISPATCH_BATCH_SIZE), start=1):
        batch_futures = run_simulation.map(job_batch, deferred=True)
        queued_futures.extend(batch_futures)
        logger.info("Queued batch %s containing %s task run(s).", batch_number, len(job_batch))

    summary = {
        "total_candidate_jobs": len(candidate_jobs),
        "skipped_existing": len(skipped_jobs),
        "queued": len(queued_jobs),
        "config": config.model_dump(mode="json", exclude_defaults=True),
    }
    logger.info("Dispatch summary: %s", summary)

    completion_summary = _wait_for_deferred_tasks(queued_futures, logger)
    summary["task_completion"] = completion_summary
    return summary


def _wait_for_deferred_tasks(queued_futures: list, logger) -> dict[str, int]:
    if not queued_futures:
        return {"completed": 0, "failed": 0, "cancelled": 0, "crashed": 0}

    total = len(queued_futures)
    previous_terminal_count = -1

    while True:
        done, not_done = wait(queued_futures, timeout=TASK_MONITOR_TIMEOUT_SECONDS)
        terminal_count = len(done)
        if terminal_count != previous_terminal_count:
            logger.info(
                "Task progress: %s/%s terminal, %s remaining.",
                terminal_count,
                total,
                len(not_done),
            )
            previous_terminal_count = terminal_count

        if not not_done:
            break

    state_counts = Counter(
        future.state.type for future in queued_futures if future.state is not None
    )
    completion_summary = {
        "completed": state_counts.get(StateType.COMPLETED, 0),
        "failed": state_counts.get(StateType.FAILED, 0),
        "cancelled": state_counts.get(StateType.CANCELLED, 0),
        "crashed": state_counts.get(StateType.CRASHED, 0),
    }

    if completion_summary["failed"] or completion_summary["cancelled"] or completion_summary["crashed"]:
        logger.warning("One or more simulation tasks did not complete successfully: %s", completion_summary)

    return completion_summary


def _batched(items: Iterable[SimulationJob], batch_size: int) -> Iterator[list[SimulationJob]]:
    iterator = iter(items)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            return
        yield batch
