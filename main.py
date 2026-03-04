from enum import Enum
from pathlib import Path
from uuid import UUID
import re

import docker
from prefect import State, flow, get_run_logger, task
from prefect.client.schemas.objects import FlowRun
from pydantic import BaseModel, Field
from prefect.artifacts import create_progress_artifact, update_progress_artifact
from prefect.runtime import flow_run

MATCH_IDS_PATH = Path(__file__).resolve().parent / "matchIds.csv"
CHUNK_SIZE = 100

# Matches: "Completed runs number 123"
COMPLETED_RE = re.compile(r"Completed runs number\s+\d+\s*$")


def get_total_scenarios(match_ids_path: Path = MATCH_IDS_PATH) -> int:
    with match_ids_path.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def run_scenario_flow_run_name() -> str:
    parameters = flow_run.parameters
    if isinstance(parameters, dict):
        config = parameters.get("config", parameters)
    else:
        config = parameters

    total_scenarios = get_total_scenarios()

    if isinstance(config, dict):
        run_type = config.get("run_type", "unknown")
        option = config.get("option", "unknown")
        start_index = config.get("start_index", 1)
        end_index = config.get("end_index", total_scenarios)
    else:
        run_type = getattr(config, "run_type", "unknown")
        option = getattr(config, "option", "unknown")
        start_index = getattr(config, "start_index", 1)
        end_index = getattr(config, "end_index", total_scenarios)

    run_type = getattr(run_type, "value", run_type)
    option = getattr(option, "value", option)
    return f"{run_type}, Option {option} ({start_index:,} - {end_index:,})"


def crash_handler(flow, flow_run: FlowRun, state: State):  # noqa: ARG001
    print(f"💥 Flow {flow_run.name!r} crashed with state {state!r}")


class RunType(str, Enum):
    lfr = "CSP: Linear Fresnel Collector"
    ptc = "CSP: Parabolic Trough Collector"
    pt = "CSP: Power Tower"
    pv = "PV"
    wind = "Wind"


class Option(str, Enum):
    A = "A"
    B = "B"
    C = "C"


class Config(BaseModel):
    run_type: RunType = Field(
        description="Select the scenario to run",
        json_schema_extra={"position": 0},
    )
    option: Option = Field(
        json_schema_extra={"position": 1},
    )
    overwrite_existing_results: bool = Field(
        default=True,
        json_schema_extra={"position": 2},
    )
    start_index: int = Field(
        ge=1,
        json_schema_extra={"position": 3},
    )
    end_index: int = Field(
        ge=1,
        json_schema_extra={"position": 4},
    )


@task(task_run_name="Process Chunk: {start_idx:,} - {end_idx:,}")
def process_chunk(
        chunk_idx: int,
        start_idx: int,
        end_idx: int,
        total_artifact_id: UUID,
        total_done_start: int,
        total_target: int,
        run_type: str,
        option: str,
):
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

    if run_type == RunType.pv.value:
        working_dir = "/onsite-energy-analysis/code/pv_tech_potential"
        script = "run_scenarios_onsite_v2.jl"
    elif run_type == RunType.wind.value:
        working_dir = "/onsite-energy-analysis/code/wind_tech_potential"
        script = "wind_onsite_parallelized.jl"
    else:
        raise ValueError(f"Unsupported run type for chunk processing: {run_type}")

    client = docker.from_env()
    container = client.containers.run(
        image="julia-xpress:1.11.9",
        working_dir=working_dir,
        command=[
            "julia",
            # "--startup-file=no"
            # "--compiled-modules=existing"
            # "--pkgimages=existing",
            "--project=.",
            script,
            "--option",
            option,
            "--start",
            str(start_idx),
            "--stop",
            str(end_idx),
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

    # Return how many were completed so the flow can carry total_done forward
    return chunk_done


@flow(
    name="Run Scenario",
    flow_run_name=run_scenario_flow_run_name,
    log_prints=True,
    on_crashed=[crash_handler],
)
def run_scenario(config: Config):
    logger = get_run_logger()
    total_scenarios = get_total_scenarios()
    start_index = config.start_index if config.start_index is not None else 1
    end_index = config.end_index if config.end_index is not None else total_scenarios

    if total_scenarios <= 0:
        raise ValueError(f"No IDs found in {MATCH_IDS_PATH}")
    if start_index > total_scenarios:
        raise ValueError(
            f"start_index ({start_index}) cannot exceed total scenarios ({total_scenarios})"
        )
    if end_index > total_scenarios:
        raise ValueError(
            f"end_index ({end_index}) cannot exceed total scenarios ({total_scenarios})"
        )
    if end_index < start_index:
        raise ValueError(
            f"end_index ({end_index}) cannot be less than start_index ({start_index})"
        )

    total_target = end_index - start_index + 1

    if config.run_type not in {RunType.pv.value, RunType.wind.value}:
        raise ValueError(f"Unimplemented run type: {config.run_type}")

    logger.info(f"Running {config.run_type!r} scenario with option {config.option!r} and config: {config}")

    total_artifact_id: UUID = create_progress_artifact(
        progress=0.0,
        key="total-progress",
        description=f"Total completed: 0 / {total_target:,}",
    )

    total_done = 0
    num_chunks = (total_target + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk_idx in range(1, num_chunks + 1):
        start_idx = start_index + (chunk_idx - 1) * CHUNK_SIZE
        end_idx = min(start_idx + CHUNK_SIZE - 1, end_index)

        print(f"Chunk {chunk_idx} of {num_chunks}")

        # Pass total artifact + current total count into the task
        chunk_done = process_chunk.submit(
            chunk_idx,
            start_idx,
            end_idx,
            total_artifact_id,
            total_done,
            total_target,
            config.run_type,
            config.option,
        ).result()

        total_done += int(chunk_done)
