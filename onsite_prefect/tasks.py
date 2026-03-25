import asyncio
import os
import signal
import socket
import threading
import time
from contextlib import suppress

from prefect import get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import StateType
from prefect.context import get_run_context
from prefect.exceptions import ObjectNotFound
from prefect.states import Cancelled, Completed, Failed, Running

from .command_builder import iter_local_result_files, remove_local_result_files, result_key_for_local_file
from .jobs import SimulationJob, SimulationResult
from .minio import job_has_existing_remote_result, upload_result_file

RUN_SIMULATION_TASK_KEY = "run_simulation"
RUN_LIFECYCLE_POLL_SECONDS = float(os.environ.get("ONSITE_RUN_LIFECYCLE_POLL_SECONDS", "10"))
PROCESS_TERMINATION_GRACE_SECONDS = float(
    os.environ.get("ONSITE_PROCESS_TERMINATION_GRACE_SECONDS", "10")
)
STOP_STATE_TYPES = {StateType.CANCELLING, StateType.CANCELLED}
JULIA_LOG_CHUNK_BYTES = 65_536

_ACTIVE_PROCESS_GROUPS: dict[int, str] = {}
_ACTIVE_PROCESS_GROUPS_LOCK = threading.Lock()


@task(
    name="run-simulation",
    task_run_name="{job.display_name}",
    retries=0,
    persist_result=False,
)
async def run_simulation(job: SimulationJob) -> SimulationResult:
    logger = get_run_logger()
    host = socket.gethostname()
    started_at = time.monotonic()
    await _persist_current_task_run_name(job, logger)

    logger.info(
        "Starting simulation for site_id=%s technology=%s option=%s host=%s",
        job.site_id,
        job.technology.name,
        job.sizing_strategy.cli_value,
        host,
    )

    if not job.overwrite_existing_results and job_has_existing_remote_result(job):
        skipped_result = SimulationResult(
            site_id=job.site_id,
            status="skipped",
            return_code=0,
            host=host,
            duration_seconds=time.monotonic() - started_at,
            uploaded_count=0,
        )
        cancelled_state = Cancelled(
            message=f"Skipped site_id={job.site_id} because results already exist in MinIO.",
            data=skipped_result,
        )
        await _persist_current_task_run_state(cancelled_state, logger)
        logger.info("Skipping site_id=%s because results already exist in MinIO.", job.site_id)
        return cancelled_state

    removed_before_run = remove_local_result_files(job)
    if removed_before_run:
        logger.warning(
            "Removed %s stale local result file(s) before starting site_id=%s.",
            len(removed_before_run),
            job.site_id,
        )

    await _persist_current_task_run_state(
        Running(message=f"Simulation started for site_id={job.site_id}."),
        logger,
    )
    process = await _start_simulation_process(job)
    _register_active_process_group(process.pid, job.site_id)

    stdout_capture_task = asyncio.create_task(_capture_pipe(process.stdout))
    stderr_capture_task = asyncio.create_task(_capture_pipe(process.stderr))
    monitor_task = asyncio.create_task(_monitor_run_lifecycle(process, logger, job))

    try:
        return_code = await process.wait()
    except asyncio.CancelledError:
        await _persist_current_task_run_state(
            Cancelled(message=f"Simulation cancelled for site_id={job.site_id}."),
            logger,
        )
        await _terminate_subprocess(
            process,
            logger,
            job.site_id,
            "task coroutine was cancelled",
        )
        raise
    except BaseException:
        await _persist_current_task_run_state(
            Failed(message=f"Simulation crashed for site_id={job.site_id}."),
            logger,
        )
        await _terminate_subprocess(
            process,
            logger,
            job.site_id,
            "task execution aborted unexpectedly",
        )
        raise
    finally:
        monitor_stop_reason = await _drain_monitor_task(monitor_task)
        stdout_output, stderr_output = await asyncio.gather(
            stdout_capture_task,
            stderr_capture_task,
            return_exceptions=False,
        )
        _unregister_active_process_group(process.pid)

    _log_captured_output(logger, job.site_id, "stdout", stdout_output, is_error=False)
    _log_captured_output(logger, job.site_id, "stderr", stderr_output, is_error=True)

    duration_seconds = time.monotonic() - started_at
    if monitor_stop_reason is not None:
        remove_local_result_files(job)
        await _persist_current_task_run_state(
            Cancelled(message=f"Simulation stopped for site_id={job.site_id}: {monitor_stop_reason}."),
            logger,
        )
        logger.warning(
            "Simulation stopped for site_id=%s because %s.",
            job.site_id,
            monitor_stop_reason,
        )
        raise RuntimeError(f"Simulation stopped for {job.site_id}: {monitor_stop_reason}")

    if return_code != 0:
        remove_local_result_files(job)
        await _persist_current_task_run_state(
            Failed(message=f"Simulation failed for site_id={job.site_id} with return code {return_code}."),
            logger,
        )
        logger.error("Simulation failed for site_id=%s with return code %s.", job.site_id, return_code)
        raise RuntimeError(f"Simulation failed for {job.site_id} with return code {return_code}")

    local_result_files = iter_local_result_files(job)
    if not local_result_files:
        skipped_result = SimulationResult(
            site_id=job.site_id,
            status="skipped",
            return_code=return_code,
            host=host,
            duration_seconds=duration_seconds,
            uploaded_count=0,
        )
        cancelled_state = Cancelled(
            message=f"Simulation completed for site_id={job.site_id} without result files.",
            data=skipped_result,
        )
        await _persist_current_task_run_state(cancelled_state, logger)
        logger.info("Simulation completed for site_id=%s without result files.", job.site_id)
        return cancelled_state

    uploaded_count = 0
    for local_result_file in local_result_files:
        result_key = result_key_for_local_file(local_result_file)
        upload_result_file(result_key, local_result_file)
        local_result_file.unlink(missing_ok=True)
        uploaded_count += 1

    remove_local_result_files(job)
    await _persist_current_task_run_state(
        Completed(message=f"Simulation completed for site_id={job.site_id} with {uploaded_count} uploaded result file(s)."),
        logger,
    )
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


async def _start_simulation_process(job: SimulationJob) -> asyncio.subprocess.Process:
    popen_kwargs = {
        "cwd": job.working_directory,
        "env": {**os.environ, **job.environment},
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.PIPE,
    }
    if os.name == "posix":
        popen_kwargs["start_new_session"] = True

    return await asyncio.create_subprocess_exec(
        *job.command,
        **popen_kwargs,
    )


async def _capture_pipe(pipe: asyncio.StreamReader | None) -> str:
    if pipe is None:
        return ""

    captured = await pipe.read()
    return captured.decode("utf-8", errors="replace")


def _log_captured_output(
    logger,
    site_id: str,
    stream_name: str,
    output: str,
    *,
    is_error: bool,
) -> None:
    stripped_output = output.strip()
    if not stripped_output:
        return

    log_function = logger.warning if is_error else logger.info
    encoded_output = stripped_output.encode("utf-8")
    if len(encoded_output) <= JULIA_LOG_CHUNK_BYTES:
        log_function("[%s][%s]\n%s", site_id, stream_name, stripped_output)
        return

    chunk_count = (len(encoded_output) + JULIA_LOG_CHUNK_BYTES - 1) // JULIA_LOG_CHUNK_BYTES
    for index, chunk in enumerate(_chunk_bytes(encoded_output, JULIA_LOG_CHUNK_BYTES), start=1):
        message = chunk.decode("utf-8", errors="replace")
        log_function(
            "[%s][%s][chunk %s/%s]\n%s",
            site_id,
            stream_name,
            index,
            chunk_count,
            message,
        )


def _chunk_bytes(content: bytes, chunk_size: int) -> list[bytes]:
    return [
        content[index:index + chunk_size]
        for index in range(0, len(content), chunk_size)
    ]


async def _monitor_run_lifecycle(
    process: asyncio.subprocess.Process,
    logger,
    job: SimulationJob,
) -> str | None:
    run_context = get_run_context()
    task_run_id = str(run_context.task_run.id)
    flow_run_id = (
        str(run_context.task_run.flow_run_id)
        if run_context.task_run.flow_run_id is not None
        else None
    )

    if flow_run_id is None:
        return None

    async with get_client() as client:
        while process.returncode is None:
            stop_reason = await _get_stop_reason(client, task_run_id, flow_run_id)
            if stop_reason is not None:
                logger.warning(
                    "Stopping simulation for site_id=%s because %s.",
                    job.site_id,
                    stop_reason,
                )
                await _terminate_subprocess(process, logger, job.site_id, stop_reason)
                return stop_reason

            await asyncio.sleep(RUN_LIFECYCLE_POLL_SECONDS)

    return None


async def _get_stop_reason(client, task_run_id: str, flow_run_id: str) -> str | None:
    try:
        task_run = await client.read_task_run(task_run_id)
    except ObjectNotFound:
        return "the task run was deleted"

    if task_run.state is not None and task_run.state.type in STOP_STATE_TYPES:
        return f"the task run entered state {task_run.state.type.value!r}"

    try:
        flow_run = await client.read_flow_run(flow_run_id)
    except ObjectNotFound:
        return "the parent flow run was deleted"

    if flow_run.state is not None and flow_run.state.type in STOP_STATE_TYPES:
        return f"the parent flow run entered state {flow_run.state.type.value!r}"

    return None


async def _drain_monitor_task(monitor_task: asyncio.Task[str | None]) -> str | None:
    if not monitor_task.done():
        monitor_task.cancel()
        with suppress(asyncio.CancelledError):
            await monitor_task
        return None

    return monitor_task.result()


def _register_active_process_group(process_group_id: int | None, site_id: str) -> None:
    if process_group_id is None:
        return

    with _ACTIVE_PROCESS_GROUPS_LOCK:
        _ACTIVE_PROCESS_GROUPS[process_group_id] = site_id


def _unregister_active_process_group(process_group_id: int | None) -> None:
    if process_group_id is None:
        return

    with _ACTIVE_PROCESS_GROUPS_LOCK:
        _ACTIVE_PROCESS_GROUPS.pop(process_group_id, None)


async def _terminate_subprocess(
    process: asyncio.subprocess.Process,
    logger,
    site_id: str,
    reason: str,
) -> None:
    if process.returncode is not None:
        return

    logger.warning("Terminating Julia process for site_id=%s because %s.", site_id, reason)
    _signal_process_group(process.pid, signal.SIGTERM)

    try:
        await asyncio.wait_for(process.wait(), timeout=PROCESS_TERMINATION_GRACE_SECONDS)
        return
    except asyncio.TimeoutError:
        logger.warning(
            "Julia process for site_id=%s did not exit after %ss; sending SIGKILL.",
            site_id,
            PROCESS_TERMINATION_GRACE_SECONDS,
        )
        _signal_process_group(process.pid, signal.SIGKILL)
        with suppress(ProcessLookupError):
            await process.wait()


def terminate_active_simulations(reason: str) -> None:
    with _ACTIVE_PROCESS_GROUPS_LOCK:
        active_process_groups = list(_ACTIVE_PROCESS_GROUPS.items())

    for process_group_id, site_id in active_process_groups:
        print(
            f"Stopping Julia process group {process_group_id} for site_id={site_id} because {reason}.",
            flush=True,
        )
        _terminate_process_group_sync(process_group_id)


def _terminate_process_group_sync(process_group_id: int) -> None:
    if not _process_exists(process_group_id):
        return

    _signal_process_group(process_group_id, signal.SIGTERM)
    deadline = time.monotonic() + PROCESS_TERMINATION_GRACE_SECONDS
    while time.monotonic() < deadline:
        if not _process_exists(process_group_id):
            return
        time.sleep(0.2)

    _signal_process_group(process_group_id, signal.SIGKILL)


def _signal_process_group(process_group_id: int | None, sig: int) -> None:
    if process_group_id is None:
        return

    try:
        if os.name == "posix":
            os.killpg(process_group_id, sig)
        else:
            os.kill(process_group_id, sig)
    except ProcessLookupError:
        return


def _process_exists(process_group_id: int) -> bool:
    try:
        os.kill(process_group_id, 0)
        return True
    except ProcessLookupError:
        return False


def _build_task_run_name(job: SimulationJob) -> str:
    return job.display_name


async def _persist_current_task_run_name(job: SimulationJob, logger) -> None:
    run_context = get_run_context()
    task_run_id = run_context.task_run.id
    task_run_name = _build_task_run_name(job)

    try:
        async with get_client() as client:
            await client.set_task_run_name(task_run_id=task_run_id, name=task_run_name)
    except ObjectNotFound:
        logger.warning(
            "Skipped renaming task_run_id=%s because it no longer exists.",
            task_run_id,
        )
    except Exception:
        logger.exception(
            "Failed to rename task_run_id=%s to %r.",
            task_run_id,
            task_run_name,
        )


async def _persist_current_task_run_state(state, logger) -> None:
    run_context = get_run_context()
    task_run_id = run_context.task_run.id

    try:
        async with get_client() as client:
            await client.set_task_run_state(
                task_run_id=task_run_id,
                state=state,
                force=True,
            )
    except ObjectNotFound:
        logger.warning(
            "Skipped persisting task state %s because task_run_id=%s no longer exists.",
            state.name,
            task_run_id,
        )
    except Exception:
        logger.exception(
            "Failed to persist task state %s for task_run_id=%s.",
            state.name,
            task_run_id,
        )


# Use a stable task key so flow workers and task workers can coordinate across
# rebuilds without relying on Prefect's code-hash-based default.
run_simulation.task_key = RUN_SIMULATION_TASK_KEY
