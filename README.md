# MCDCe-performance

Electron benchmark set for comparing the optimized `MCDC` tree across:

- pure Python
- serial Numba
- Numba CPU parallel (MPI ranks)
- Numba GPU target

The cases are lightweight Lockwood-style slab problems for three elements:

- `Al`
- `Cu`
- `U`

## Local driver

You can still run the Python driver directly:

```bash
cd /usr/workspace/derman1/tuo/ACE-work/test/MCDC-P-electron
export MCDC_VV_PROCESS_DATA_LIBRARY_DIR=/path/to/electron-vv-data
python3 run.py
```

Useful overrides:

```bash
python3 run.py \
  --cases Al Cu \
  --n-particles 1000 10000 100000 \
  --cpu-ranks 4 \
  --gpu-ranks 1 \
  --launcher auto
```

Notes:

- `CPU parallel` here means multiple MPI ranks running `--mode=numba --target=cpu`.
- `GPU parallel` means `--mode=numba --target=gpu`; if a Flux allocation is active, the driver uses `flux run`.
- The driver prepends the configured `MCDC` source tree to `PYTHONPATH`, so runs target the optimized local source tree by default.
- Results go under `results/<timestamp>/` with per-run logs and a `summary.csv`.

## Flux

The recommended entry point is the dispatcher:

```bash
./submit_flux.sh
```

That script submits one Flux job per `(element, N_PARTICLE, backend)` combination.

Example:

- `3` elements: `Al Cu U`
- `2` particle counts: `1000 10000`
- `4` backends: `python`, `numba`, `numba_cpu_parallel`, `numba_gpu_parallel`

This gives `3 x 2 x 4 = 24` independent Flux jobs, and they can run concurrently as resources become available.

The scripts currently default to these site paths:

- `REPO_ROOT=/usr/workspace/derman1/tuo/ACE-work/test/MCDC-P-electron`
- `MCDC_ROOT=/usr/workspace/derman1/tuo/ACE-work/test/MCDC`
- `MCDC_VV_PROCESS_DATA_LIBRARY_DIR=/usr/workspace/derman1/tuo/ACE-work/test/mcdc-lib`

Useful overrides:

```bash
CPU_QUEUE=pbatch GPU_QUEUE=pbatch \
CPU_RANKS=4 GPU_RANKS=1 \
N_PARTICLES="1000 10000" \
./submit_flux.sh
```

Walltime defaults are intentionally conservative:

- `CPU_TIME=08h`
- `GPU_TIME=08h`

You can shorten them at submit time, for example:

```bash
CPU_QUEUE=pbatch GPU_QUEUE=pbatch \
CPU_TIME=02h GPU_TIME=01h \
N_PARTICLES="1000 10000" \
./submit_flux.sh
```

If `./submit_flux.sh` gives `permission denied`, either make the scripts executable:

```bash
chmod u+x submit_flux.sh submit_cpu_flux.sh submit_gpu_flux.sh
```

or run with:

```bash
bash submit_flux.sh
```

To inspect queues on the system:

```bash
flux queue list
flux queue status pbatch
```

If `pbatch` is marked with `*`, it is the default queue, so explicitly setting `CPU_QUEUE=pbatch GPU_QUEUE=pbatch` is optional.

## Outputs

Each dispatcher call creates a dispatch directory:

```text
results/dispatch_<timestamp>/
```

Inside it:

- `flux_logs/*.out` and `flux_logs/*.err`: per-job Flux stdout/stderr logs
- `job_results/<case>/<backend>/N<particle>/`: per-job run outputs
- `submitted_jobs.tsv`: manifest of submitted jobs, job ids, result paths, and log paths

If you want to submit just one worker directly:

```bash
CASE=Al BACKEND=python N_PARTICLE=1000 flux batch -N1 -n1 submit_cpu_flux.sh
CASE=Al BACKEND=numba_gpu_parallel N_PARTICLE=1000 flux batch -N1 -n1 -g1 submit_gpu_flux.sh
```
