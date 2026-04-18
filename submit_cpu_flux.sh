#!/bin/bash
# Flux batch worker for one (element, N, backend) CPU benchmark.
# Intended to be submitted by submit_flux.sh, but can also be submitted directly:
#   CASE=Al BACKEND=python N_PARTICLE=1000 REPO_ROOT=/path/to/MCDC-P-electron flux batch -N1 -n1 submit_cpu_flux.sh

#flux: -N 1
#flux: -n 4
#flux: -t 08h
#flux: --job-name=mcdc-p-cpu
#flux: --output=mcdc-p-cpu.out
#flux: --error=mcdc-p-cpu.err

set -euo pipefail

DEFAULT_REPO_ROOT="/usr/workspace/derman1/tuo/ACE-work/test/MCDC-P-electron"
DEFAULT_MCDC_ROOT="/usr/workspace/derman1/tuo/ACE-work/test/MCDC"
DEFAULT_DATA_LIBRARY="/usr/workspace/derman1/tuo/ACE-work/test/mcdc-lib"

REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
CASE="${CASE:?CASE must be set, e.g. CASE=Al}"
BACKEND="${BACKEND:?BACKEND must be set (python|numba|numba_cpu_parallel)}"
N_PARTICLE="${N_PARTICLE:?N_PARTICLE must be set}"
CPU_RANKS="${CPU_RANKS:-4}"
MCDC_ROOT="${MCDC_ROOT:-$DEFAULT_MCDC_ROOT}"
MCDC_VV_PROCESS_DATA_LIBRARY_DIR="${MCDC_VV_PROCESS_DATA_LIBRARY_DIR:-$DEFAULT_DATA_LIBRARY}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/results/cpu_${CASE}_${BACKEND}_N${N_PARTICLE}_${FLUX_JOB_ID:-$(date +%Y%m%d_%H%M%S)}}"

case "$BACKEND" in
  python|numba|numba_cpu_parallel)
    ;;
  *)
    echo "Unsupported CPU backend: $BACKEND" >&2
    exit 1
    ;;
esac

export MCDC_VV_PROCESS_DATA_LIBRARY_DIR
export MCDC_LIB="$MCDC_VV_PROCESS_DATA_LIBRARY_DIR"

cd "$REPO_ROOT"

echo "Workflow      : CPU"
echo "Host          : $(hostname)"
echo "Date          : $(date)"
echo "Case          : $CASE"
echo "Python        : $(command -v "$PYTHON_BIN")"
echo "Repo root     : $REPO_ROOT"
echo "MCDC root     : $MCDC_ROOT"
echo "Data library  : $MCDC_VV_PROCESS_DATA_LIBRARY_DIR"
echo "Backend       : $BACKEND"
echo "N_PARTICLE    : $N_PARTICLE"
echo "CPU ranks     : $CPU_RANKS"
echo "Results dir   : $RESULTS_DIR"

"$PYTHON_BIN" "$REPO_ROOT/run.py" \
  --cases "$CASE" \
  --n-particles "$N_PARTICLE" \
  --backends "$BACKEND" \
  --cpu-ranks "$CPU_RANKS" \
  --launcher flux \
  --results-dir "$RESULTS_DIR" \
  --mcdc-root "$MCDC_ROOT" \
  --data-library "$MCDC_VV_PROCESS_DATA_LIBRARY_DIR"
