import os

from prefect.task_worker import serve

from .tasks import run_simulation


def main() -> None:
    serve(
        run_simulation,
        limit=int(os.environ.get("PREFECT_TASK_WORKER_LIMIT", "108")),
    )


if __name__ == "__main__":
    main()
