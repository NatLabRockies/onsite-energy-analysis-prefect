import os

from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
from prefect.exceptions import ObjectNotFound
from prefect.types.entrypoint import EntrypointType

from onsite_prefect.flows import dispatch_simulations
from onsite_prefect.maintenance import cleanup_task_scheduler_storage
from onsite_prefect.task_storage import ensure_task_scheduling_storage_block


def apply_deployment(flow, *, name: str, work_pool_name: str, **deployment_kwargs) -> None:
    deployment = flow.to_deployment(
        name=name,
        work_pool_name=work_pool_name,
        entrypoint_type=EntrypointType.MODULE_PATH,
        **deployment_kwargs,
    )

    with get_client(sync_client=True) as client:
        try:
            existing = client.read_deployment_by_name(f"{flow.name}/{name}")
        except ObjectNotFound:
            existing = None

        if existing is not None and existing.path is not None:
            client.delete_deployment(existing.id)

    deployment.apply(work_pool_name=work_pool_name)


def main() -> None:
    deployment_name = os.environ.get("PREFECT_DEPLOYMENT_NAME", "Onsite Energy Scenario")
    work_pool_name = os.environ.get("PREFECT_WORK_POOL", "julia-pool")

    ensure_task_scheduling_storage_block()

    apply_deployment(
        dispatch_simulations,
        name=deployment_name,
        work_pool_name=work_pool_name,
    )
    apply_deployment(
        cleanup_task_scheduler_storage,
        name=os.environ.get("PREFECT_TASK_CLEANUP_DEPLOYMENT_NAME", "Task Scheduler Cleanup"),
        work_pool_name=work_pool_name,
        schedule=CronSchedule(
            cron=os.environ.get("PREFECT_TASK_CLEANUP_CRON", "0 3 * * *"),
            timezone=os.environ.get("PREFECT_TASK_CLEANUP_TIMEZONE", "America/Denver"),
        ),
        parameters={
            "older_than_days": int(os.environ.get("PREFECT_TASK_CLEANUP_OLDER_THAN_DAYS", "3")),
        },
    )


if __name__ == "__main__":
    main()
