import asyncio
import os

from prefect.task_worker import TaskWorker

from .task_storage import OnsiteMinioProxyStorage
from .tasks import RUN_SIMULATION_TASK_KEY, run_simulation, terminate_active_simulations


def _configure_task_worker(limit: int) -> TaskWorker:
    worker = TaskWorker(run_simulation, limit=limit)
    original_handle_sigterm = worker.handle_sigterm

    def handle_sigterm(signum: int, frame: object) -> None:
        terminate_active_simulations("the task worker received SIGTERM")
        original_handle_sigterm(signum, frame)

    for task in worker.tasks:
        task.task_key = RUN_SIMULATION_TASK_KEY

    worker.task_keys = {RUN_SIMULATION_TASK_KEY}
    worker.in_flight_task_runs = {RUN_SIMULATION_TASK_KEY: {}}
    worker.finished_task_runs = {RUN_SIMULATION_TASK_KEY: 0}
    worker.handle_sigterm = handle_sigterm
    return worker


async def amain() -> None:
    limit = int(os.environ.get("PREFECT_TASK_WORKER_LIMIT", "108"))
    worker = _configure_task_worker(limit=limit)
    print(f"Starting task worker for task_key={RUN_SIMULATION_TASK_KEY}", flush=True)
    await worker.astart()


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
