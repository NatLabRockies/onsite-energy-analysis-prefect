from enum import Enum
from uuid import UUID
import re

import docker
from prefect import State, flow, get_run_logger, task
from prefect.client.schemas.objects import FlowRun
from pydantic import BaseModel
from prefect.artifacts import create_progress_artifact, update_progress_artifact

TOTAL_SCENARIOS = 169_755
CHUNK_SIZE = 100

# Matches: "Completed runs number 123"
COMPLETED_RE = re.compile(r"Completed runs number\s+\d+\s*$")


def crash_handler(flow, flow_run: FlowRun, state: State):  # noqa: ARG001
    print(f"💥 Flow {flow_run.name!r} crashed with state {state!r}")


class RunType(str, Enum):
    onsite = "onsite"
    pv = "pv"
    wind = "wind"


class Config(BaseModel):
    run_type: RunType = RunType.onsite
    overwrite_existing_results: bool = False


@task(task_run_name="Process Chunk: {start_idx:,} - {end_idx:,}")
def process_chunk(chunk_idx: int, start_idx: int, end_idx: int, total_artifact_id: UUID, total_done_start: int):
    logger = get_run_logger()
    chunk_size = end_idx - start_idx + 1

    chunk_key = f"chunk-progress-{chunk_idx:03d}"
    chunk_artifact_id = create_progress_artifact(
        progress=0.0,
        key=chunk_key,
        description=f"Chunk {chunk_idx:,}: 0 / {chunk_size:,} (indices {start_idx:,}-{end_idx:,})",
    )

    total_done = total_done_start
    chunk_done = 0

    client = docker.from_env()
    container = client.containers.run(
        image="julia-xpress:1.11.9",
        working_dir="/onsite-energy-analysis/code/PV_Tech_Potential",
        command=[
            "julia",
            "--project=@.",
            "run_scenarios_onsite_v2.jl",
        ],
        hostname="aswindle-133446-w1",
        mac_address="00:4e:01:fc:67:61",
        volumes={
            "/home/aswindle/docker/onsite-energy/iedo00onsite_data": {
                "bind": "/data",
                "mode": "ro",
            },
            "/home/aswindle/docker/onsite-energy/onsite-energy-analysis": {
                "bind": "/onsite-energy-analysis",
                "mode": "rw",
            },
            "/home/aswindle/docker/onsite-energy/julia-cache": {
                "bind": "/root/.julia",
                "mode": "rw",
            },
        },
        environment={
            "START_INDEX": str(start_idx),
            "END_INDEX": str(end_idx),
            "NREL_DEVELOPER_API_KEY": "qpq9xqtFtxbLXd9nhpQ9zaovrGpFb5ynnpfqxL7B",
        },
        detach=True,
        remove=True,
    )

    for raw in container.logs(stream=True, follow=True):
        line = raw.decode("utf-8", errors="replace").rstrip()
        logger.info(line)

        if COMPLETED_RE.search(line):
            total_done += 1
            chunk_done += 1

            update_progress_artifact(
                artifact_id=total_artifact_id,
                progress=100.0 * total_done / TOTAL_SCENARIOS,
                description=f"Total completed: {total_done:,} / {TOTAL_SCENARIOS:,}",
            )

            update_progress_artifact(
                artifact_id=chunk_artifact_id,
                progress=100.0 * chunk_done / chunk_size,
                description=f"Chunk {chunk_idx:,}: {chunk_done:,} / {chunk_size:,} (indices {start_idx:,}-{end_idx:,})",
            )

    code = container.wait()["StatusCode"]
    if code != 0:
        raise RuntimeError(code)

    # Return how many were completed so the flow can carry total_done forward
    return chunk_done


@flow(
    name="Run Scenario",
    log_prints=True,
    on_crashed=[crash_handler],
)
def run_scenario(config: Config):
    logger = get_run_logger()

    if config.run_type != RunType.onsite:
        raise ValueError(f"Unsupported run type: {config.run_type}")

    logger.info(f"Running scenario with config: {config}")

    total_artifact_id: UUID = create_progress_artifact(
        progress=0.0,
        key="total-progress",
        description=f"Total completed: 0 / {TOTAL_SCENARIOS:,}",
    )

    total_done = 0
    num_chunks = (TOTAL_SCENARIOS + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk_idx in range(1, num_chunks + 1):
        start_idx = (chunk_idx - 1) * CHUNK_SIZE + 1
        end_idx = min(chunk_idx * CHUNK_SIZE, TOTAL_SCENARIOS)

        print(f"Chunk {chunk_idx} of {num_chunks}")

        # Pass total artifact + current total count into the task
        chunk_done = process_chunk.submit(
            chunk_idx, start_idx, end_idx, total_artifact_id, total_done
        ).result()

        total_done += int(chunk_done)
