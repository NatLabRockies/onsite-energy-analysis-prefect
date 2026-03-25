import time
from collections import Counter
from itertools import islice
from typing import Any, Iterable, Iterator

from prefect import State, flow, get_run_logger
from prefect.artifacts import create_progress_artifact, update_progress_artifact
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterFlowRunId, TaskRunFilterState, \
    TaskRunFilterStateType
from prefect.client.schemas.objects import FlowRun, StateType
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
TASK_MONITOR_POLL_SECONDS = 15


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


@flow(
    name="dispatch-simulations",
    flow_run_name=dispatch_simulations_flow_run_name,
    log_prints=True,
    on_crashed=[crash_handler],
)
def dispatch_simulations(config: Config) -> dict[str, Any]:
    logger = get_run_logger()

    validate_config_data(config)
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

    progress_artifact_id = create_progress_artifact(
        progress=_calculate_progress_percent(0, len(queued_futures)),
        description=_build_progress_description(0, len(queued_futures)),
    )

    completion_summary = _wait_for_deferred_tasks(
        flow_run.id,
        len(queued_futures),
        progress_artifact_id,
        logger,
    )
    summary["task_completion"] = completion_summary
    return summary


def _wait_for_deferred_tasks(
    current_flow_run_id: str | None,
    total_queued_tasks: int,
    progress_artifact_id,
    logger,
) -> dict[str, int]:
    if total_queued_tasks == 0:
        return {"completed": 0, "failed": 0, "cancelled": 0, "crashed": 0}
    if current_flow_run_id is None:
        raise ValueError("Current flow run id is unavailable for deferred task monitoring.")

    previous_terminal_count = -1

    while True:
        state_counts = _read_task_run_state_counts(current_flow_run_id)
        terminal_count = sum(
            state_counts.get(state_type, 0)
            for state_type in (StateType.COMPLETED, StateType.FAILED, StateType.CANCELLED, StateType.CRASHED)
        )

        if terminal_count != previous_terminal_count:
            logger.info(
                "Task progress: %s/%s terminal, running=%s pending=%s scheduled=%s.",
                terminal_count,
                total_queued_tasks,
                state_counts.get(StateType.RUNNING, 0),
                state_counts.get(StateType.PENDING, 0),
                state_counts.get(StateType.SCHEDULED, 0),
            )
            update_progress_artifact(
                artifact_id=progress_artifact_id,
                progress=_calculate_progress_percent(terminal_count, total_queued_tasks),
                description=_build_progress_description(terminal_count, total_queued_tasks),
            )
            previous_terminal_count = terminal_count

        if terminal_count >= total_queued_tasks:
            break

        time.sleep(TASK_MONITOR_POLL_SECONDS)

    completion_summary = {
        "completed": state_counts.get(StateType.COMPLETED, 0),
        "failed": state_counts.get(StateType.FAILED, 0),
        "cancelled": state_counts.get(StateType.CANCELLED, 0),
        "crashed": state_counts.get(StateType.CRASHED, 0),
    }

    if completion_summary["failed"] or completion_summary["crashed"]:
        logger.warning("One or more simulation tasks did not complete successfully: %s", completion_summary)
    elif completion_summary["cancelled"]:
        logger.info("One or more simulation tasks were intentionally skipped or cancelled: %s", completion_summary)

    return completion_summary


def _calculate_progress_percent(completed_count: int, total_count: int) -> float:
    if total_count <= 0:
        return 100.0
    return (completed_count / total_count) * 100.0


def _build_progress_description(completed_count: int, total_count: int) -> str:
    return f"Completed simulations: {completed_count}/{total_count}"


def _read_task_run_state_counts(current_flow_run_id: str) -> Counter[StateType]:
    state_counts: Counter[StateType] = Counter()
    monitored_state_types = (
        StateType.RUNNING,
        StateType.PENDING,
        StateType.SCHEDULED,
        StateType.COMPLETED,
        StateType.FAILED,
        StateType.CANCELLED,
        StateType.CRASHED,
    )

    with get_client(sync_client=True) as client:
        for state_type in monitored_state_types:
            state_counts[state_type] = _count_task_runs(
                client,
                current_flow_run_id,
                state_type,
            )

    return state_counts


def _count_task_runs(client, current_flow_run_id: str, state_type: StateType) -> int:
    body = {
        "task_runs": TaskRunFilter(
            flow_run_id=TaskRunFilterFlowRunId(any_=[current_flow_run_id]),
            state=TaskRunFilterState(
                type=TaskRunFilterStateType(any_=[state_type]),
                name=None,
            ),
        ).model_dump(mode="json", exclude_unset=True, exclude_none=True),
    }
    response = client._client.post("/task_runs/count", json=body)  # noqa: SLF001
    return int(response.json())


def _batched(items: Iterable[SimulationJob], batch_size: int) -> Iterator[list[SimulationJob]]:
    iterator = iter(items)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            return
        yield batch
