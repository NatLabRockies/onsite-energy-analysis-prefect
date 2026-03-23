from itertools import islice
from typing import Any, Iterable, Iterator

from prefect import State, flow, get_run_logger, task
from prefect.client.schemas.objects import FlowRun
from prefect.runtime import flow_run
from pydantic import ValidationError

from .command_builder import build_simulation_jobs
from .config import Config, build_flow_run_name, coerce_config, validate_config as validate_config_data, \
    get_requested_match_ids
from .jobs import SimulationJob
from .minio import filter_existing_jobs
from .tasks import run_simulation

DISPATCH_BATCH_SIZE = 100


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

    for batch_number, job_batch in enumerate(_batched(queued_jobs, DISPATCH_BATCH_SIZE), start=1):
        run_simulation.map(job_batch, deferred=True)
        logger.info("Queued batch %s containing %s task run(s).", batch_number, len(job_batch))

    summary = {
        "total_candidate_jobs": len(candidate_jobs),
        "skipped_existing": len(skipped_jobs),
        "queued": len(queued_jobs),
        "config": config.model_dump(mode="json", exclude_defaults=True),
    }
    logger.info("Dispatch summary: %s", summary)
    return summary


def _batched(items: Iterable[SimulationJob], batch_size: int) -> Iterator[list[SimulationJob]]:
    iterator = iter(items)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            return
        yield batch
