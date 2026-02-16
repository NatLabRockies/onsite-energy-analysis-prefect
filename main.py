from enum import Enum

import docker
from prefect import State, flow, get_run_logger, task
from prefect.client.schemas.objects import FlowRun
from pydantic import BaseModel

TOTAL_SCENARIOS = 169_755


def crash_handler(flow, flow_run: FlowRun, state: State):  # noqa: ARG001
    print(f"💥 Flow {flow_run.name!r} crashed with state {state!r}")


class RunType(str, Enum):
    onsite = "onsite"
    pv = "pv"
    wind = "wind"


class Config(BaseModel):
    run_type: RunType = RunType.onsite
    overwrite_existing_results: bool = False


@task
def process_chunk(start_idx: int, end_idx: int):
    logger = get_run_logger()

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

    for line in container.logs(stream=True):
        logger.info(line.decode("utf-8", errors="replace").rstrip())

    code = container.wait()["StatusCode"]
    if code != 0:
        raise RuntimeError(code)


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

    ids = list(range(1, TOTAL_SCENARIOS + 1))
    chunk_size = 1000
    chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    for i, chunk in enumerate(chunks, 1):
        print(f"Chunk {i} of {len(chunks)}")
        process_chunk.submit(chunk[0], chunk[-1]).result()
