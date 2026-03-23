# Building

1. Copy the appropriate `xpauth-aswindle-133446-w1.xpr` or `xpauth-aswindle-133447-w1.xpr` file based on the system hostname into the `xpress` directory first, and rename it to `xpauth.xpr`.
2. Run `build-image.sh`.

# Worker Layout

- Julia repo mounted at `/onsite-energy-analysis`
- data mounted at `/data` (read-only)
- results written under `/onsite-energy-analysis/results`
- Julia package cache mounted at `/root/.julia`

Before starting a worker, copy [user_paths.jl](./user_paths.jl) into the mounted Julia repo at `code/common/user_paths.jl`:

Use [compose.worker.env.example](../compose.worker.env.example) as the template for a per-host `.env` file, then start the worker with:

```bash
docker compose --env-file /path/to/worker.env -f compose.worker.yml up -d
```
