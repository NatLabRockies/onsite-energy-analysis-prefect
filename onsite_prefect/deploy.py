import os

from prefect.client.orchestration import get_client
from prefect.exceptions import ObjectNotFound
from prefect.types.entrypoint import EntrypointType

from .flows import dispatch_simulations
from .task_storage import ensure_task_scheduling_storage_block


def main() -> None:
    deployment_name = os.environ.get("PREFECT_DEPLOYMENT_NAME", "Onsite Energy Scenario")
    work_pool_name = os.environ.get("PREFECT_WORK_POOL", "julia-pool")

    ensure_task_scheduling_storage_block()

    deployment = dispatch_simulations.to_deployment(
        name=deployment_name,
        work_pool_name=work_pool_name,
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    with get_client(sync_client=True) as client:
        try:
            existing = client.read_deployment_by_name(f"{dispatch_simulations.name}/{deployment_name}")
        except ObjectNotFound:
            existing = None

        if existing is not None and existing.path is not None:
            client.delete_deployment(existing.id)

    deployment.apply(work_pool_name=work_pool_name)


if __name__ == "__main__":
    main()