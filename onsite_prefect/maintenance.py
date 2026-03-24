from datetime import datetime, timedelta, timezone
from typing import Any

from prefect import flow, get_run_logger

from .minio import delete_object, iter_listed_objects
from .task_storage import TASK_SCHEDULING_STORAGE_PREFIX

TASK_SCHEDULING_PARAMETERS_PREFIX = f"{TASK_SCHEDULING_STORAGE_PREFIX}/parameters/"


@flow(name="task-scheduler-cleanup", log_prints=True)
def cleanup_task_scheduler_storage(older_than_days: int = 3) -> dict[str, Any]:
    logger = get_run_logger()
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)

    scanned = 0
    deleted = 0
    already_missing = 0

    logger.info(
        "Scanning MinIO prefix %s for task-scheduling parameter objects older than %s.",
        TASK_SCHEDULING_PARAMETERS_PREFIX,
        cutoff.isoformat(),
    )

    for listed_object in iter_listed_objects(TASK_SCHEDULING_PARAMETERS_PREFIX):
        scanned += 1
        if listed_object.last_modified >= cutoff:
            continue

        if delete_object(listed_object.key):
            deleted += 1
        else:
            already_missing += 1

    summary = {
        "prefix": TASK_SCHEDULING_PARAMETERS_PREFIX,
        "older_than_days": older_than_days,
        "cutoff": cutoff.isoformat(),
        "scanned": scanned,
        "deleted": deleted,
        "already_missing": already_missing,
    }
    logger.info("Task scheduler cleanup summary: %s", summary)
    return summary
