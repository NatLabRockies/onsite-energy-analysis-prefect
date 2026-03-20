from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final
from uuid import UUID

import docker
from prefect import get_run_logger, task
from prefect.artifacts import create_progress_artifact, update_progress_artifact

from .config import SizingStrategy, Technology

COMPLETED_RE: Final[re.Pattern[str]] = re.compile(r"Completed runs number\s+\d+\s*$")


@dataclass(frozen=True)
class ContainerSpec:
    working_dir: str
    script: str


CONTAINER_SPECS: Final[dict[Technology, ContainerSpec]] = {
    Technology.pv: ContainerSpec(
        working_dir="/onsite-energy-analysis/code/pv_tech_potential",
        script="run_scenarios_onsite_v2.jl",
    ),
    Technology.wind: ContainerSpec(
        working_dir="/onsite-energy-analysis/code/wind_tech_potential",
        script="wind_onsite_parallelized.jl",
    ),
}

DOCKER_VOLUMES: Final[dict[str, dict[str, str]]] = {
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
}

DOCKER_ENVIRONMENT: Final[dict[str, str]] = {
    "NREL_DEVELOPER_API_KEY": "gAXbkyLjfTFEFfiO3YhkxxJ6rkufRaSktk40ho4x",
}


def get_container_spec(technology: Technology) -> ContainerSpec:
    try:
        return CONTAINER_SPECS[technology]
    except KeyError as exc:
        raise ValueError(f"Unsupported run type for chunk processing: {technology.value}") from exc


@task(task_run_name="Process Chunk: {start_idx:,} - {end_idx:,}")
def process_chunk(
    chunk_idx: int,
    start_idx: int,
    end_idx: int,
    total_artifact_id: UUID,
    total_done_start: int,
    total_target: int,
    technology: Technology,
    sizing_strategy: SizingStrategy,
) -> int:
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
    container_spec = get_container_spec(technology)

    client = docker.from_env()
    container = client.containers.run(
        image="julia-xpress:1.11.9",
        working_dir=container_spec.working_dir,
        command=[
            "julia",
            "--project=.",
            container_spec.script,
            "--option",
            sizing_strategy.cli_value,
            "--start",
            str(start_idx),
            "--stop",
            str(end_idx),
        ],
        hostname="aswindle-133446-w1",
        mac_address="00:4e:01:fc:67:61",
        volumes=DOCKER_VOLUMES,
        environment=DOCKER_ENVIRONMENT,
        detach=True,
        remove=True,
    )

    for raw in container.logs(stream=True, follow=True):
        line = raw.decode("utf-8", errors="replace").rstrip()
        logger.info(line)

        if not COMPLETED_RE.search(line):
            continue

        total_done += 1
        chunk_done += 1

        update_progress_artifact(
            artifact_id=total_artifact_id,
            progress=100.0 * total_done / total_target,
            description=f"Total completed: {total_done:,} / {total_target:,}",
        )

        update_progress_artifact(
            artifact_id=chunk_artifact_id,
            progress=100.0 * chunk_done / chunk_size,
            description=f"Chunk {chunk_idx:,}: {chunk_done:,} / {chunk_size:,} (indices {start_idx:,}-{end_idx:,})",
        )

    code = container.wait()["StatusCode"]
    if code != 0:
        raise RuntimeError(code)

    return chunk_done
