import re
from dataclasses import dataclass
from typing import Final, Iterable, Iterator
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from .config import SizingStrategy, Technology

MINIO_RESULTS_BUCKET_URL: Final = "http://bball-130449.nrel.gov:4200/minio/onsite-results/"
LIST_BUCKET_XML_NAMESPACE: Final = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
LIST_PAGE_SIZE: Final = 1000
HTTP_TIMEOUT_SECONDS: Final = 30

TECHNOLOGY_RESULT_DIRECTORIES: Final[dict[Technology, str]] = {
    Technology.lfr: "fresnel",
    Technology.pt: "mst",
    Technology.ptc: "trough",
    Technology.pv: "pv",
    Technology.wind: "wind",
}

RESULT_KEY_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    # CSP
    re.compile(r"(?:^|/)result_(?P<match_id>[A-Za-z0-9]{8})_[A-Za-z]+\.csv$"),
    # PV and wind
    re.compile(r"(?:^|/)(?P<match_id>[A-Za-z0-9]{8})_[A-Za-z]+_run_result\.csv$"),
)


@dataclass(frozen=True)
class MatchIdFilterResult:
    requested_match_ids: tuple[str, ...]
    pending_match_ids: tuple[str, ...]
    existing_match_ids: frozenset[str]
    searched_prefixes: tuple[str, ...]


def get_results_prefixes(technology: Technology, sizing_strategy: SizingStrategy) -> tuple[str, ...]:
    result_directory = TECHNOLOGY_RESULT_DIRECTORIES[technology]
    option = sizing_strategy.cli_value
    return (f"{result_directory}/option {option}/",)


def build_list_bucket_request(prefix: str, continuation_token: str | None = None) -> Request:
    params = {
        "list-type": "2",
        "prefix": prefix,
        "max-keys": str(LIST_PAGE_SIZE),
    }
    if continuation_token:
        params["continuation-token"] = continuation_token

    query = urlencode(params, quote_via=quote)
    return Request(f"{MINIO_RESULTS_BUCKET_URL}?{query}", method="GET")


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


def extract_match_id_from_key(key: str) -> str | None:
    for pattern in RESULT_KEY_PATTERNS:
        match = pattern.search(key)
        if match:
            return match.group("match_id")

    return None


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

    selected_match_ids = set(requested_match_ids_tuple)
    remaining_match_ids = set(requested_match_ids_tuple)
    existing_match_ids: set[str] = set()
    searched_prefixes = get_results_prefixes(technology, sizing_strategy)

    for prefix in searched_prefixes:
        for key in iter_listed_object_keys(prefix):
            match_id = extract_match_id_from_key(key)
            if match_id is None or match_id not in selected_match_ids or match_id in existing_match_ids:
                continue

            existing_match_ids.add(match_id)
            remaining_match_ids.discard(match_id)

            if not remaining_match_ids:
                break

        if not remaining_match_ids:
            break

    pending_match_ids = tuple(
        match_id for match_id in requested_match_ids_tuple if match_id not in existing_match_ids
    )

    return MatchIdFilterResult(
        requested_match_ids=requested_match_ids_tuple,
        pending_match_ids=pending_match_ids,
        existing_match_ids=frozenset(existing_match_ids),
        searched_prefixes=searched_prefixes,
    )
