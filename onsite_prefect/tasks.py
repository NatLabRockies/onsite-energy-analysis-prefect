import os
import socket
import subprocess
import threading
import time

from prefect import get_run_logger, task

from .command_builder import iter_local_result_files, remove_local_result_files, result_key_for_local_file
from .jobs import SimulationJob, SimulationResult
from .minio import job_has_existing_remote_result, upload_result_file


@task(
    name="run-simulation",
    task_run_name="run-simulation {job.technology.name} {job.sizing_strategy.cli_value} {job.site_id}",
    retries=0,
    persist_result=False,
)
def run_simulation(job: SimulationJob) -> SimulationResult:
    logger = get_run_logger()
    host = socket.gethostname()
    started_at = time.monotonic()

    logger.info(
        "Starting simulation for site_id=%s technology=%s option=%s host=%s",
        job.site_id,
        job.technology.name,
        job.sizing_strategy.cli_value,
        host,
    )

    if not job.overwrite_existing_results and job_has_existing_remote_result(job):
        logger.info("Skipping site_id=%s because results already exist in MinIO.", job.site_id)
        return SimulationResult(
            site_id=job.site_id,
            status="skipped",
            return_code=0,
            host=host,
            duration_seconds=time.monotonic() - started_at,
            uploaded_count=0,
        )

    removed_before_run = remove_local_result_files(job)
    if removed_before_run:
        logger.warning(
            "Removed %s stale local result file(s) before starting site_id=%s.",
            len(removed_before_run),
            job.site_id,
        )

    process = subprocess.Popen(
        job.command,
        cwd=job.working_directory,
        env={**os.environ, **job.environment},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    stdout_thread = threading.Thread(
        target=_stream_pipe,
        args=(process.stdout, logger.info, job.site_id, "stdout"),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_stream_pipe,
        args=(process.stderr, logger.warning, job.site_id, "stderr"),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    return_code = process.wait()
    stdout_thread.join()
    stderr_thread.join()

    duration_seconds = time.monotonic() - started_at
    if return_code != 0:
        remove_local_result_files(job)
        logger.error("Simulation failed for site_id=%s with return code %s.", job.site_id, return_code)
        raise RuntimeError(f"Simulation failed for {job.site_id} with return code {return_code}")

    local_result_files = iter_local_result_files(job)
    if not local_result_files:
        logger.info("Simulation completed for site_id=%s without result files.", job.site_id)
        return SimulationResult(
            site_id=job.site_id,
            status="skipped",
            return_code=return_code,
            host=host,
            duration_seconds=duration_seconds,
            uploaded_count=0,
        )

    uploaded_count = 0
    for local_result_file in local_result_files:
        result_key = result_key_for_local_file(local_result_file)
        upload_result_file(result_key, local_result_file)
        local_result_file.unlink(missing_ok=True)
        uploaded_count += 1

    remove_local_result_files(job)
    logger.info(
        "Simulation completed for site_id=%s with %s uploaded result file(s).",
        job.site_id,
        uploaded_count,
    )
    return SimulationResult(
        site_id=job.site_id,
        status="completed",
        return_code=return_code,
        host=host,
        duration_seconds=duration_seconds,
        uploaded_count=uploaded_count,
    )


def _stream_pipe(pipe, log_function, site_id: str, stream_name: str) -> None:
    if pipe is None:
        return

    try:
        for line in pipe:
            message = line.rstrip()
            if message:
                log_function("[%s][%s] %s", site_id, stream_name, message)
    finally:
        pipe.close()
