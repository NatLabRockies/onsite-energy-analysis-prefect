from __future__ import annotations

from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from prefect._internal.compatibility.async_dispatch import async_dispatch
from prefect.filesystems import WritableFileSystem
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from .minio import HTTP_TIMEOUT_SECONDS, MINIO_RESULTS_BUCKET_URL

TASK_SCHEDULING_STORAGE_PREFIX = "_prefect/task-scheduling"
TASK_SCHEDULING_STORAGE_BLOCK_NAME = "onsite-task-scheduling"


class OnsiteMinioProxyStorage(WritableFileSystem):
    _block_type_name = "Onsite MinIO Proxy Storage"
    _documentation_url = "https://docs.prefect.io/v3/advanced/background-tasks"

    bucket_url: str = MINIO_RESULTS_BUCKET_URL
    key_prefix: str = TASK_SCHEDULING_STORAGE_PREFIX

    def _event_method_called_resources(self):
        return None

    def _build_object_key(self, path: str) -> str:
        normalized_path = path.strip().lstrip("/")
        if not normalized_path:
            raise ValueError("Path must not be empty.")

        prefix = self.key_prefix.strip().strip("/")
        return f"{prefix}/{normalized_path}" if prefix else normalized_path

    def _build_object_url(self, path: str) -> str:
        object_key = self._build_object_key(path)
        return f"{self.bucket_url.rstrip('/')}/{quote(object_key, safe='/')}"

    def _request(self, method: str, path: str, data: bytes | None = None) -> bytes:
        headers: dict[str, str] = {}
        if data is not None:
            headers = {
                "Content-Length": str(len(data)),
                "Content-Type": "application/octet-stream",
            }

        request = Request(
            self._build_object_url(path),
            data=data,
            method=method,
            headers=headers,
        )

        try:
            with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return response.read()
        except HTTPError as exc:
            if exc.code == 404:
                raise ValueError(f"Path {path} does not exist.") from exc
            raise

    def _path_exists(self, path: str) -> bool:
        request = Request(self._build_object_url(path), method="HEAD")
        try:
            with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS):
                return True
        except HTTPError as exc:
            if exc.code == 404:
                return False
            raise

    async def aread_path(self, path: str) -> bytes:
        if not await run_sync_in_worker_thread(self._path_exists, path):
            raise ValueError(f"Path {path} does not exist.")
        return await run_sync_in_worker_thread(self._request, "GET", path)

    @async_dispatch(aread_path)
    def read_path(self, path: str) -> bytes:
        if not self._path_exists(path):
            raise ValueError(f"Path {path} does not exist.")
        return self._request("GET", path)

    async def awrite_path(self, path: str, content: bytes) -> str:
        await run_sync_in_worker_thread(self._request, "PUT", path, content)
        return self._build_object_key(path)

    @async_dispatch(awrite_path)
    def write_path(self, path: str, content: bytes) -> str:
        self._request("PUT", path, content)
        return self._build_object_key(path)


def get_task_scheduling_storage_block_slug() -> str:
    return (
        f"{OnsiteMinioProxyStorage.get_block_type_slug()}"
        f"/{TASK_SCHEDULING_STORAGE_BLOCK_NAME}"
    )


def ensure_task_scheduling_storage_block() -> None:
    OnsiteMinioProxyStorage().save(
        name=TASK_SCHEDULING_STORAGE_BLOCK_NAME,
        overwrite=True,
    )
