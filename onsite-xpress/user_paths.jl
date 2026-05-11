#=
user_paths_template.jl - Template for Local Path Configuration

INSTRUCTIONS:
1. Copy this file to "user_paths.jl" in the same directory
2. Edit the paths below to match your local data locations
3. Do NOT commit user_paths.jl to git (it's already in .gitignore)

For detailed instructions, see: PATHS_SETUP_INSTRUCTIONS.md
=#
const PROJECT_BASE_PATH = normpath(joinpath(@__DIR__, "..", ".."))
const DATA_BASE_PATH = "/data"

# =============================================================================
# Electric Load Data
# =============================================================================

# Directory containing electric load profile CSVs from IEDO Teams
# These are the facility load profile files (e.g., facility_*.csv)
const ELECTRIC_LOAD_PATH = joinpath(DATA_BASE_PATH, "facility_load_profiles")

# =============================================================================
# Natural Gas Data
# =============================================================================

# Directory containing natural gas consumption data
# This is the annual energy consumption data from the Analysis Team
const NG_LOAD_PATH = joinpath(DATA_BASE_PATH, "facility_annual_energy_consumption")

# =============================================================================
# Baseload Size Data
# =============================================================================

# Directory containing baseload size data from the master files
# Located in the Teams "Baseline Site, Parcel, Usage, and Profile Data" folder
const BASELOAD_PATH = joinpath(DATA_BASE_PATH, "baseload sizes")

# =============================================================================
# PV Production Factors (PVWatts Data)
# =============================================================================

# These directories contain pre-generated PVWatts simulation outputs
# organized by mounting configuration

# Roof-mounted PV production factors
const PV_ROOF_PROD_PATH = joinpath(DATA_BASE_PATH, "pvwatts_roof_csvs")

# Ground-mounted fixed-tilt PV production factors
const PV_GROUND_FIXED_PATH = joinpath(DATA_BASE_PATH, "pvwatts_ground_fixed_csvs")

# Ground-mounted single-axis tracking PV production factors
const PV_GROUND_AXIS_PATH = joinpath(DATA_BASE_PATH, "pvwatts_ground_axis_csvs")

# =============================================================================
# Wind Resource Data
# =============================================================================

# Directory containing wind resource data (pickle files)
const WIND_RESOURCE_PATH = joinpath(DATA_BASE_PATH, "wind_resource_data")

# Directory for cached wind production profiles (auto-generated, can be in repo)
const WIND_PROD_CACHE_PATH = joinpath(DATA_BASE_PATH, "wind_production_profiles")

# =============================================================================
# Parcel/Site List Files
# =============================================================================

# Full path to the PV parcel/site list CSV file
# This is the master facility parcels file from PNNL
const PARCEL_FILE_PV = joinpath(
    DATA_BASE_PATH,
    "pnnl_parcel_land_coverage_data",
    "updated_4_10_2026",
    "aggregated_facility_level_site_list_2026_04_23.csv",
)

# Full path to the Wind parcel/site list CSV file
# This is the wind-specific site list with exclusions applied
const PARCEL_FILE_WIND = joinpath(
    DATA_BASE_PATH,
    "pnnl_parcel_land_coverage_data",
    "wind_sitelist",
    "wind_sitelist_3x7_no_sites_with_exclusions.csv",
)

# Full path to the CSP parcel/site list CSV file
# This is the CSP-specific site list with exclusions applied
const PARCEL_FILE_CSP = joinpath(
    DATA_BASE_PATH,
    "pnnl_parcel_land_coverage_data",
    "csp_sites_2026",
    "sites_with_2micoastal_flag_04_24_2026.csv",
)

# =============================================================================
# Results Output
# =============================================================================

# Base directory where results will be written
# Subdirectories will be created automatically (e.g., PV/option A/, Wind/A/normal/)
# This can be within the repo (it's git-ignored) or anywhere on your machine
const RESULTS_BASE_PATH = "/onsite-energy-analysis/results"
