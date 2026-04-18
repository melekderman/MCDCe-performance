#!/bin/bash
# Dispatcher script: submit one Flux job per (element, N, backend) combination.
# Run this on the submit/login node:
#   ./submit_flux.sh
#
# Optional overrides:
#   CASES="Al Cu U"
#   N_PARTICLES="1000 10000 100000"
#   BACKENDS="python numba numba_cpu_parallel numba_gpu_parallel"
#   CPU_QUEUE=pbatch
#   GPU_QUEUE=pdebug
#   CPU_RANKS=4
#   GPU_RANKS=1
#   CPU_TIME=08h
#   GPU_TIME=02h
#   DRY_RUN=1

set -euo pipefail

DEFAULT_REPO_ROOT="/usr/workspace/derman1/tuo/ACE-work/test/MCDC-P-electron"
DEFAULT_MCDC_ROOT="/usr/workspace/derman1/tuo/ACE-work/test/MCDC"
DEFAULT_DATA_LIBRARY="/usr/workspace/derman1/tuo/ACE-work/test/mcdc-lib"

REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
CASES="${CASES:-Al Cu U}"
N_PARTICLES="${N_PARTICLES:-1000 10000 100000}"
BACKENDS="${BACKENDS:-python numba numba_cpu_parallel numba_gpu_parallel}"
CPU_QUEUE="${CPU_QUEUE:-}"
GPU_QUEUE="${GPU_QUEUE:-}"
CPU_RANKS="${CPU_RANKS:-4}"
GPU_RANKS="${GPU_RANKS:-1}"
CPU_TIME="${CPU_TIME:-08h}"
GPU_TIME="${GPU_TIME:-08h}"
MCDC_ROOT="${MCDC_ROOT:-$DEFAULT_MCDC_ROOT}"
MCDC_VV_PROCESS_DATA_LIBRARY_DIR="${MCDC_VV_PROCESS_DATA_LIBRARY_DIR:-$DEFAULT_DATA_LIBRARY}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DRY_RUN="${DRY_RUN:-0}"
SUBMIT_TAG="${SUBMIT_TAG:-$(date +%Y%m%d_%H%M%S)}"

CPU_SCRIPT="$REPO_ROOT/submit_cpu_flux.sh"
GPU_SCRIPT="$REPO_ROOT/submit_gpu_flux.sh"
DISPATCH_ROOT="$REPO_ROOT/results/dispatch_$SUBMIT_TAG"
LOG_ROOT="$DISPATCH_ROOT/flux_logs"
JOB_RESULTS_ROOT="$DISPATCH_ROOT/job_results"
MANIFEST="$DISPATCH_ROOT/submitted_jobs.tsv"

if ! command -v flux >/dev/null 2>&1; then
  echo "flux command not found in PATH" >&2
  exit 1
fi

if [[ ! -f "$CPU_SCRIPT" ]]; then
  echo "CPU submit script not found: $CPU_SCRIPT" >&2
  exit 1
fi

if [[ ! -f "$GPU_SCRIPT" ]]; then
  echo "GPU submit script not found: $GPU_SCRIPT" >&2
  exit 1
fi

if [[ ! -d "$MCDC_ROOT" ]]; then
  echo "MCDC root not found: $MCDC_ROOT" >&2
  exit 1
fi

if [[ ! -d "$MCDC_VV_PROCESS_DATA_LIBRARY_DIR" ]]; then
  echo "Electron data library not found: $MCDC_VV_PROCESS_DATA_LIBRARY_DIR" >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python executable not found: $PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$LOG_ROOT" "$JOB_RESULTS_ROOT"
printf 'jobid\tcase\tn_particles\tbackend\tworkflow\tqueue\tresults_dir\tstdout\tstderr\n' > "$MANIFEST"

submit_job() {
  local workflow="$1"
  local case_name="$2"
  local n_particles="$3"
  local backend="$4"
  local queue_name="$5"
  local ranks="$6"
  local walltime="$7"
  local script_path="$8"
  local workflow_lc
  workflow_lc="$(printf '%s' "$workflow" | tr '[:upper:]' '[:lower:]')"
  local tag="${case_name}_N${n_particles}_${backend}"
  local job_name="mcdc-${case_name}-N${n_particles}-${backend}"
  local results_dir="$JOB_RESULTS_ROOT/$case_name/$backend/N${n_particles}"
  local stdout_log="$LOG_ROOT/${tag}.out"
  local stderr_log="$LOG_ROOT/${tag}.err"
  local -a cmd=(
    flux batch
    -N 1
    -n "$ranks"
    -t "$walltime"
    --job-name "$job_name"
    --output "$stdout_log"
    --error "$stderr_log"
  )

  if [[ -n "$queue_name" ]]; then
    cmd+=(--queue "$queue_name")
  fi

  if [[ "$workflow" == "GPU" ]]; then
    cmd+=(-g 1)
  fi

  cmd+=("$script_path")

  if [[ "$DRY_RUN" == "1" ]]; then
    printf '[dry-run] CASE=%s N=%s backend=%s workflow=%s -> ' "$case_name" "$n_particles" "$backend" "$workflow"
    printf '%q ' env \
      REPO_ROOT="$REPO_ROOT" \
      CASE="$case_name" \
      BACKEND="$backend" \
      N_PARTICLE="$n_particles" \
      CPU_RANKS="$CPU_RANKS" \
      GPU_RANKS="$GPU_RANKS" \
      RESULTS_DIR="$results_dir" \
      MCDC_ROOT="$MCDC_ROOT" \
      MCDC_VV_PROCESS_DATA_LIBRARY_DIR="$MCDC_VV_PROCESS_DATA_LIBRARY_DIR" \
      PYTHON_BIN="$PYTHON_BIN"
    printf '%q ' "${cmd[@]}"
    printf '\n'
    return 0
  fi

  env \
    REPO_ROOT="$REPO_ROOT" \
    CASE="$case_name" \
    BACKEND="$backend" \
    N_PARTICLE="$n_particles" \
    CPU_RANKS="$CPU_RANKS" \
    GPU_RANKS="$GPU_RANKS" \
    RESULTS_DIR="$results_dir" \
    MCDC_ROOT="$MCDC_ROOT" \
    MCDC_VV_PROCESS_DATA_LIBRARY_DIR="$MCDC_VV_PROCESS_DATA_LIBRARY_DIR" \
    PYTHON_BIN="$PYTHON_BIN" \
    "${cmd[@]}"
}

echo "Repo root     : $REPO_ROOT"
echo "MCDC root     : $MCDC_ROOT"
echo "Data library  : $MCDC_VV_PROCESS_DATA_LIBRARY_DIR"
echo "Cases         : $CASES"
echo "N_PARTICLES   : $N_PARTICLES"
echo "Backends      : $BACKENDS"
echo "CPU queue     : ${CPU_QUEUE:-<default>}"
echo "GPU queue     : ${GPU_QUEUE:-<default>}"
echo "CPU ranks     : $CPU_RANKS"
echo "GPU ranks     : $GPU_RANKS"
echo "Dispatch root : $DISPATCH_ROOT"

for case_name in $CASES; do
  for n_particles in $N_PARTICLES; do
    for backend in $BACKENDS; do
      case "$backend" in
        python|numba)
          workflow="CPU"
          queue_name="$CPU_QUEUE"
          ranks="1"
          walltime="$CPU_TIME"
          script_path="$CPU_SCRIPT"
          ;;
        numba_cpu_parallel)
          workflow="CPU"
          queue_name="$CPU_QUEUE"
          ranks="$CPU_RANKS"
          walltime="$CPU_TIME"
          script_path="$CPU_SCRIPT"
          ;;
        numba_gpu_parallel)
          workflow="GPU"
          queue_name="$GPU_QUEUE"
          ranks="$GPU_RANKS"
          walltime="$GPU_TIME"
          script_path="$GPU_SCRIPT"
          ;;
        *)
          echo "Unsupported backend: $backend" >&2
          exit 1
          ;;
      esac

      jobid="$(submit_job "$workflow" "$case_name" "$n_particles" "$backend" "$queue_name" "$ranks" "$walltime" "$script_path")"
      echo -e "${jobid}\t${case_name}\t${n_particles}\t${backend}\t${workflow}\t${queue_name:-<default>}\t$JOB_RESULTS_ROOT/$case_name/$backend/N${n_particles}\t$LOG_ROOT/${case_name}_N${n_particles}_${backend}.out\t$LOG_ROOT/${case_name}_N${n_particles}_${backend}.err" >> "$MANIFEST"
      echo "Submitted ${workflow} job: case=$case_name N=$n_particles backend=$backend jobid=$jobid"
    done
  done
done
