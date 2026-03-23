import mimetypes
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from .command_builder import build_exact_result_keys
from .config import SizingStrategy, Technology
from .jobs import SimulationJob

MINIO_RESULTS_BUCKET_URL = "http://bball-130449.nrel.gov:4200/minio/onsite-results"
LIST_BUCKET_XML_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
LIST_PAGE_SIZE = 1000
HTTP_TIMEOUT_SECONDS = 30
WIND_RESULT_SUFFIX = "_Wind_run_result.csv"


@dataclass(frozen=True)
class MatchIdFilterResult:
    requested_match_ids: tuple[str, ...]
    pending_match_ids: tuple[str, ...]
    existing_match_ids: frozenset[str]
    searched_prefixes: tuple[str, ...]


def build_object_url(object_key: str) -> str:
    return f"{MINIO_RESULTS_BUCKET_URL}/{quote(object_key, safe='/')}"


def build_list_bucket_request(prefix: str, continuation_token: str | None = None) -> Request:
    params = {
        "list-type": "2",
        "prefix": prefix,
        "max-keys": str(LIST_PAGE_SIZE),
    }
    if continuation_token:
        params["continuation-token"] = continuation_token

    query = urlencode(params, quote_via=quote)
    return Request(f"{MINIO_RESULTS_BUCKET_URL}/?{query}", method="GET")


def object_exists(object_key: str) -> bool:
    request = Request(build_object_url(object_key), method="HEAD")
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS):
            return True
    except HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def iter_listed_object_keys(prefix: str) -> Iterator[str]:
    continuation_token: str | None = None

    while True:
        request = build_list_bucket_request(prefix, continuation_token)
        with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            root = ElementTree.fromstring(response.read())

        for key_element in root.findall("s3:Contents/s3:Key", LIST_BUCKET_XML_NAMESPACE):
            if key_element.text:
                yield key_element.text

        is_truncated = (
            root.findtext("s3:IsTruncated", default="false", namespaces=LIST_BUCKET_XML_NAMESPACE) == "true"
        )
        continuation_token = root.findtext(
            "s3:NextContinuationToken",
            default="",
            namespaces=LIST_BUCKET_XML_NAMESPACE,
        )

        if not is_truncated or not continuation_token:
            return


def filter_existing_match_ids(
    technology: Technology,
    sizing_strategy: SizingStrategy,
    requested_match_ids: Iterable[str],
    *,
    overwrite_existing_results: bool,
) -> MatchIdFilterResult:
    requested_match_ids_tuple = tuple(requested_match_ids)
    if overwrite_existing_results or not requested_match_ids_tuple:
        return MatchIdFilterResult(
            requested_match_ids=requested_match_ids_tuple,
            pending_match_ids=requested_match_ids_tuple,
            existing_match_ids=frozenset(),
            searched_prefixes=tuple(),
        )

    if technology is Technology.wind:
        prefix = f"wind/option {sizing_strategy.cli_value}/"
        existing_match_ids = find_existing_wind_site_ids(prefix, set(requested_match_ids_tuple))
        searched_prefixes = (prefix,)
    else:
        existing_match_ids = {
            match_id
            for match_id in requested_match_ids_tuple
            if any(
                object_exists(result_key)
                for result_key in build_exact_result_keys(match_id, technology, sizing_strategy)
            )
        }
        searched_prefixes = tuple()

    pending_match_ids = tuple(
        match_id for match_id in requested_match_ids_tuple if match_id not in existing_match_ids
    )
    return MatchIdFilterResult(
        requested_match_ids=requested_match_ids_tuple,
        pending_match_ids=pending_match_ids,
        existing_match_ids=frozenset(existing_match_ids),
        searched_prefixes=searched_prefixes,
    )


def filter_existing_jobs(jobs: list[SimulationJob]) -> tuple[list[SimulationJob], list[SimulationJob]]:
    queued_jobs: list[SimulationJob] = []
    skipped_jobs: list[SimulationJob] = []
    wind_jobs_by_prefix: dict[str, list[SimulationJob]] = {}

    for job in jobs:
        if job.overwrite_existing_results:
            queued_jobs.append(job)
        elif job.technology is Technology.wind:
            wind_jobs_by_prefix.setdefault(job.result_prefix, []).append(job)
        elif any(object_exists(result_key) for result_key in job.exact_result_keys):
            skipped_jobs.append(job)
        else:
            queued_jobs.append(job)

    for prefix, grouped_jobs in wind_jobs_by_prefix.items():
        existing_site_ids = find_existing_wind_site_ids(prefix, {job.site_id for job in grouped_jobs})
        for job in grouped_jobs:
            if job.site_id in existing_site_ids:
                skipped_jobs.append(job)
            else:
                queued_jobs.append(job)

    return queued_jobs, skipped_jobs


def job_has_existing_remote_result(job: SimulationJob) -> bool:
    if job.overwrite_existing_results:
        return False

    if job.technology is Technology.wind:
        return job.site_id in find_existing_wind_site_ids(job.result_prefix, {job.site_id})

    return any(object_exists(result_key) for result_key in job.exact_result_keys)


def find_existing_wind_site_ids(prefix: str, requested_site_ids: set[str]) -> set[str]:
    existing_site_ids: set[str] = set()
    remaining_site_ids = set(requested_site_ids)

    for object_key in iter_listed_object_keys(prefix):
        site_id = extract_wind_site_id(object_key)
        if site_id is None:
            continue

        if site_id not in remaining_site_ids:
            continue

        existing_site_ids.add(site_id)
        remaining_site_ids.discard(site_id)
        if not remaining_site_ids:
            break

    return existing_site_ids


def extract_wind_site_id(object_key: str) -> str | None:
    object_name = Path(object_key).name
    if not object_name.endswith(WIND_RESULT_SUFFIX):
        return None

    return object_name.removesuffix(WIND_RESULT_SUFFIX)


def upload_result_file(result_key: str, local_path: Path) -> None:
    content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
    payload = local_path.read_bytes()
    request = Request(
        build_object_url(result_key),
        data=payload,
        method="PUT",
        headers={
            "Content-Length": str(len(payload)),
            "Content-Type": content_type,
        },
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS):
        return
