# Prefect Orchestration for Onsite Energy Analysis

## Accessing MinIO for simulation results
http://bball-130449.nrel.gov:10001/browser/onsite-results

For more control (e.g., bulk downloads), login with username and password `onsite-energy`

## Compiling the sysimage
_This assumes that no flows are currently running_

```bash
docker run --rm --entrypoint /bin/bash -e NREL_DEVELOPER_API_KEY=gAXbkyLjfTFEFfiO3YhkxxJ6rkufRaSktk40ho4x -v ~/docker/onsite-energy/julia-cache:/root/.julia/ -v ~/docker/onsite-energy/iedo00onsite_data:/data -v ~/docker/onsite-energy/onsite-energy-analysis:/onsite-energy-analysis onsite-prefect-worker:latest -c '
  set -euo pipefail
  apt-get update
  apt-get install -y --no-install-recommends build-essential
  # Only use the following line for a clean build
  # rm -rf /root/.julia/*
  julia --project=/onsite-energy-analysis/code/sysimage /onsite-energy-analysis/code/sysimage/build_sysimage.jl
'
```
