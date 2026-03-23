import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from .config import Config, SizingStrategy, Technology
from .jobs import SimulationJob


@dataclass(frozen=True)
class TechnologyRuntime:
    working_directory: Path
    script_path: Path
    results_subdirectory: str
    output_filename_suffix: str | None


def get_project_root() -> Path:
    return Path(os.environ.get("ONSITE_PROJECT_ROOT", "/onsite-energy-analysis"))


def get_code_root() -> Path:
    return get_project_root() / "code"


def get_results_root() -> Path:
    return get_project_root() / "results"


def get_sysimage_project_dir() -> Path:
    return get_code_root() / "sysimage"


@cache
def get_sysimage_path() -> Path:
    sysimages_dir = get_sysimage_project_dir() / "sysimages"
    candidates = sorted(
        path for path in sysimages_dir.glob("onsite_sysimage.*") if path.is_file() and path.name != ".gitignore"
    )
    if not candidates:
        raise FileNotFoundError(f"No onsite sysimage found in {sysimages_dir}")

    return candidates[0]


TECHNOLOGY_RUNTIMES: dict[Technology, TechnologyRuntime] = {
    Technology.lfr: TechnologyRuntime(
        working_directory=get_code_root() / "csp_tech_potential",
        script_path=get_code_root() / "csp_tech_potential" / "lfr-csp_parallelized.jl",
        results_subdirectory="fresnel",
        output_filename_suffix="lfr",
    ),
    Technology.ptc: TechnologyRuntime(
        working_directory=get_code_root() / "csp_tech_potential",
        script_path=get_code_root() / "csp_tech_potential" / "ptc-csp_parallelized.jl",
        results_subdirectory="trough",
        output_filename_suffix="trough",
    ),
    Technology.pt: TechnologyRuntime(
        working_directory=get_code_root() / "csp_tech_potential",
        script_path=get_code_root() / "csp_tech_potential" / "pt-csp_parallelized.jl",
        results_subdirectory="mst",
        output_filename_suffix="pt",
    ),
    Technology.pv: TechnologyRuntime(
        working_directory=get_code_root() / "pv_tech_potential",
        script_path=get_code_root() / "pv_tech_potential" / "run_scenarios_onsite_v2.jl",
        results_subdirectory="pv",
        output_filename_suffix=None,
    ),
    Technology.wind: TechnologyRuntime(
        working_directory=get_code_root() / "wind_tech_potential",
        script_path=get_code_root() / "wind_tech_potential" / "wind_onsite_parallelized.jl",
        results_subdirectory="wind",
        output_filename_suffix=None,
    ),
}


def get_result_prefix(technology: Technology, sizing_strategy: SizingStrategy) -> str:
    runtime = TECHNOLOGY_RUNTIMES[technology]
    return f"{runtime.results_subdirectory}/option {sizing_strategy.cli_value}/"


def get_local_results_root(technology: Technology, sizing_strategy: SizingStrategy) -> Path:
    runtime = TECHNOLOGY_RUNTIMES[technology]
    return get_results_root() / runtime.results_subdirectory / f"option {sizing_strategy.cli_value}"


def build_exact_result_keys(
    site_id: str,
    technology: Technology,
    sizing_strategy: SizingStrategy,
) -> list[str]:
    runtime = TECHNOLOGY_RUNTIMES[technology]
    prefix = get_result_prefix(technology, sizing_strategy)

    if technology is Technology.wind:
        return []

    if technology is Technology.pv:
        return [f"{prefix}{site_id}_PV_run_result.csv"]

    return [f"{prefix}result_{site_id}_{runtime.output_filename_suffix}.csv"]


def build_simulation_job(site_id: str, config: Config) -> SimulationJob:
    runtime = TECHNOLOGY_RUNTIMES[config.technology]
    sysimage_path = get_sysimage_path()

    command = [
        "julia",
        "--startup-file=no",
        f"--project={get_sysimage_project_dir()}",
        "-J",
        str(sysimage_path),
        str(runtime.script_path),
        "--option",
        config.sizing_strategy.cli_value,
        "--match-ids",
        site_id,
        "--workers",
        "0",
    ]

    return SimulationJob(
        site_id=site_id,
        technology=config.technology,
        sizing_strategy=config.sizing_strategy,
        overwrite_existing_results=config.overwrite_existing_results,
        working_directory=str(runtime.working_directory),
        command=command,
        result_prefix=get_result_prefix(config.technology, config.sizing_strategy),
        exact_result_keys=build_exact_result_keys(site_id, config.technology, config.sizing_strategy),
        metadata={
            "script_path": str(runtime.script_path),
            "sysimage_path": str(sysimage_path),
        },
    )


def build_simulation_jobs(config: Config, site_ids: list[str]) -> list[SimulationJob]:
    return [build_simulation_job(site_id, config) for site_id in site_ids]


def iter_local_result_files(job: SimulationJob) -> list[Path]:
    local_results_root = get_local_results_root(job.technology, job.sizing_strategy)

    if job.exact_result_keys:
        results_root = get_results_root()
        return [
            local_path
            for local_path in (results_root / Path(result_key) for result_key in job.exact_result_keys)
            if local_path.is_file()
        ]

    return sorted(local_results_root.rglob(f"{job.site_id}_Wind_run_result.csv"))


def result_key_for_local_file(local_path: Path) -> str:
    return local_path.relative_to(get_results_root()).as_posix()


def remove_local_result_files(job: SimulationJob) -> list[Path]:
    removed_paths: list[Path] = []
    for local_path in iter_local_result_files(job):
        local_path.unlink(missing_ok=True)
        removed_paths.append(local_path)
        _remove_empty_parent_directories(local_path.parent, stop_at=get_local_results_root(job.technology, job.sizing_strategy))

    return removed_paths


def _remove_empty_parent_directories(path: Path, *, stop_at: Path) -> None:
    current = path
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent
