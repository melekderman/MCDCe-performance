#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
CASES_ROOT = REPO_ROOT / "cases"
DEFAULT_MCDC_ROOT = "/usr/workspace/derman1/tuo/ACE-work/test/MCDC"
DEFAULT_RESULTS_BASE = REPO_ROOT / "results"
DEFAULT_DATA_LIBRARY = REPO_ROOT / "mcdc-lib"
DEFAULT_CASES = ["Al", "Cu", "U"]
DEFAULT_N_PARTICLES = [1_000, 10_000, 100_000]
DEFAULT_BACKENDS = [
    "python",
    "numba",
    "numba_cpu_parallel",
    "numba_gpu_parallel",
]
PROCESS_DATA_LIBRARY_ENV = "MCDC_VV_PROCESS_DATA_LIBRARY_DIR"
N_PARTICLES_ENV = "MCDC_ELECTRON_N_PARTICLES"
OUTPUT_NAME_ENV = "MCDC_ELECTRON_OUTPUT_NAME"
FLUX_ENV_NAMES = ("FLUX_URI", "FLUX_JOB_ID", "FLUX_JOB_SIZE")


@dataclass(frozen=True)
class BackendSpec:
    name: str
    mode: str
    target: str
    ranks: int
    gpus_per_rank: int = 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the MCDC-P-electron benchmark set across multiple backends."
    )
    parser.add_argument(
        "--cases",
        nargs="+",
        default=DEFAULT_CASES,
        help=f"Case names under {CASES_ROOT}. Default: {DEFAULT_CASES}",
    )
    parser.add_argument(
        "--n-particles",
        type=int,
        nargs="+",
        default=DEFAULT_N_PARTICLES,
        help=f"Particle-count sweep. Default: {DEFAULT_N_PARTICLES}",
    )
    parser.add_argument(
        "--backends",
        nargs="+",
        default=DEFAULT_BACKENDS,
        choices=DEFAULT_BACKENDS,
        help=f"Backends to run. Default: {DEFAULT_BACKENDS}",
    )
    parser.add_argument(
        "--cpu-ranks",
        type=int,
        default=4,
        help="MPI rank count for numba_cpu_parallel. Default: 4",
    )
    parser.add_argument(
        "--gpu-ranks",
        type=int,
        default=1,
        help="Rank count for numba_gpu_parallel. Default: 1",
    )
    parser.add_argument(
        "--launcher",
        choices=["auto", "local", "mpiexec", "flux"],
        default="auto",
        help="Parallel launcher selection. Default: auto",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable used for each run. Default: current interpreter",
    )
    parser.add_argument(
        "--mcdc-root",
        default=str(DEFAULT_MCDC_ROOT),
        help=f"Path to the optimized MCDC source tree. Default: {DEFAULT_MCDC_ROOT}",
    )
    parser.add_argument(
        "--data-library",
        default=os.environ.get(PROCESS_DATA_LIBRARY_ENV, str(DEFAULT_DATA_LIBRARY)),
        help=(
            "Path to electron process data. Default: "
            "MCDC_VV_PROCESS_DATA_LIBRARY_DIR or ./electron-vv-data"
        ),
    )
    parser.add_argument(
        "--results-dir",
        default=None,
        help="Directory for logs and outputs. Default: results/<timestamp>",
    )
    parser.add_argument(
        "--gpu-block-count",
        type=int,
        default=240,
        help="Pass-through for --gpu_block_count in GPU runs. Default: 240",
    )
    parser.add_argument(
        "--gpu-arena-size",
        type=int,
        default=0x100000,
        help="Pass-through for --gpu_arena_size in GPU runs. Default: 0x100000",
    )
    parser.add_argument(
        "--gpu-strategy",
        choices=["async", "event"],
        default="event",
        help="Pass-through for --gpu_strategy in GPU runs. Default: event",
    )
    parser.add_argument(
        "--gpu-state-storage",
        choices=["separate", "managed", "united"],
        default="separate",
        help="Pass-through for --gpu_state_storage in GPU runs. Default: separate",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop the sweep after the first failed run.",
    )
    return parser


def discover_cases() -> dict[str, Path]:
    case_map: dict[str, Path] = {}
    for input_path in sorted(CASES_ROOT.glob("*/input.py")):
        case_map[input_path.parent.name] = input_path.resolve()
    return case_map


def resolve_cases(requested_cases: list[str]) -> dict[str, Path]:
    discovered = discover_cases()
    normalized = {name.lower(): name for name in discovered}
    selected: dict[str, Path] = {}
    for raw_name in requested_cases:
        key = raw_name.lower()
        if key not in normalized:
            available = ", ".join(sorted(discovered))
            raise FileNotFoundError(f"Unknown case '{raw_name}'. Available cases: {available}")
        actual_name = normalized[key]
        selected[actual_name] = discovered[actual_name]
    return selected


def resolve_backends(args: argparse.Namespace) -> list[BackendSpec]:
    backend_map = {
        "python": BackendSpec("python", "python", "cpu", 1, 0),
        "numba": BackendSpec("numba", "numba", "cpu", 1, 0),
        "numba_cpu_parallel": BackendSpec(
            "numba_cpu_parallel", "numba", "cpu", max(1, args.cpu_ranks), 0
        ),
        "numba_gpu_parallel": BackendSpec(
            "numba_gpu_parallel", "numba", "gpu", max(1, args.gpu_ranks), 1
        ),
    }
    return [backend_map[name] for name in args.backends]


def make_results_dir(args: argparse.Namespace) -> Path:
    if args.results_dir:
        results_dir = Path(args.results_dir).expanduser().resolve()
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = (DEFAULT_RESULTS_BASE / stamp).resolve()
    (results_dir / "logs").mkdir(parents=True, exist_ok=True)
    (results_dir / "runs").mkdir(parents=True, exist_ok=True)
    return results_dir


def inside_flux_allocation() -> bool:
    return any(os.environ.get(name) for name in FLUX_ENV_NAMES)


def choose_launcher(requested: str, backend: BackendSpec) -> str:
    if requested != "auto":
        return requested
    if backend.ranks <= 1 and backend.target == "cpu":
        return "local"
    if inside_flux_allocation() and shutil.which("flux"):
        return "flux"
    if backend.ranks > 1 and shutil.which("mpiexec"):
        return "mpiexec"
    if backend.target == "gpu":
        return "local"
    raise RuntimeError(
        f"No usable launcher found for backend '{backend.name}'. "
        "Use --launcher or make mpiexec/flux available."
    )


def build_command(
    args: argparse.Namespace, backend: BackendSpec, launcher: str, input_path: Path
) -> list[str]:
    base_cmd = [
        args.python,
        str(input_path),
        f"--mode={backend.mode}",
        f"--target={backend.target}",
        "--output=output",
        "--no-progress_bar",
    ]

    if backend.target == "gpu":
        base_cmd.extend(
            [
                f"--gpu_strategy={args.gpu_strategy}",
                f"--gpu_state_storage={args.gpu_state_storage}",
                f"--gpu_block_count={args.gpu_block_count}",
                f"--gpu_arena_size={args.gpu_arena_size}",
            ]
        )

    if launcher == "local":
        if backend.ranks > 1 and backend.target == "cpu":
            raise RuntimeError(
                f"Backend '{backend.name}' needs more than one rank, but launcher=local."
            )
        return base_cmd

    if launcher == "mpiexec":
        if shutil.which("mpiexec") is None:
            raise RuntimeError("Requested launcher 'mpiexec', but mpiexec is not available.")
        return ["mpiexec", "-n", str(backend.ranks), *base_cmd]

    if launcher == "flux":
        if shutil.which("flux") is None:
            raise RuntimeError("Requested launcher 'flux', but flux is not available.")
        flux_cmd = ["flux", "run", "-n", str(backend.ranks)]
        if backend.gpus_per_rank:
            flux_cmd.extend(["-g", str(backend.gpus_per_rank)])
        return [*flux_cmd, *base_cmd]

    raise ValueError(f"Unsupported launcher '{launcher}'")


def build_env(args: argparse.Namespace, mcdc_root: Path, data_library: Path, n_particles: int) -> dict[str, str]:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(mcdc_root)
        if not existing_pythonpath
        else os.pathsep.join([str(mcdc_root), existing_pythonpath])
    )
    env[PROCESS_DATA_LIBRARY_ENV] = str(data_library)
    env["MCDC_LIB"] = str(data_library)
    env[N_PARTICLES_ENV] = str(n_particles)
    env[OUTPUT_NAME_ENV] = "output"
    return env


def write_metadata(
    path: Path,
    args: argparse.Namespace,
    selected_cases: dict[str, Path],
    backends: list[BackendSpec],
    mcdc_root: Path,
    data_library: Path,
) -> None:
    payload = {
        "created_at": datetime.now().isoformat(),
        "cwd": str(REPO_ROOT),
        "mcdc_root": str(mcdc_root),
        "data_library": str(data_library),
        "cases": list(selected_cases),
        "n_particles": list(args.n_particles),
        "backends": [backend.__dict__ for backend in backends],
        "launcher": args.launcher,
        "python": args.python,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_csv(rows: list[dict[str, object]], path: Path) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    selected_cases = resolve_cases(args.cases)
    backends = resolve_backends(args)
    results_dir = make_results_dir(args)
    logs_dir = results_dir / "logs"
    runs_dir = results_dir / "runs"
    mcdc_root = Path(args.mcdc_root).expanduser().resolve()
    data_library = Path(args.data_library).expanduser().resolve()

    if not mcdc_root.exists():
        raise FileNotFoundError(f"MCDC root does not exist: {mcdc_root}")
    if not args.dry_run and not data_library.exists():
        raise FileNotFoundError(
            f"Electron data library does not exist: {data_library}\n"
            "Set --data-library or MCDC_VV_PROCESS_DATA_LIBRARY_DIR to the correct path."
        )

    write_metadata(
        results_dir / "metadata.json",
        args,
        selected_cases,
        backends,
        mcdc_root,
        data_library,
    )

    print(f"Results dir  : {results_dir}")
    print(f"MCDC root    : {mcdc_root}")
    print(f"Data library : {data_library}")
    print(f"Cases        : {', '.join(selected_cases)}")
    print(f"Backends     : {', '.join(args.backends)}")
    print(f"N particles  : {', '.join(str(value) for value in args.n_particles)}")

    rows: list[dict[str, object]] = []
    failures = 0

    for case_name, input_path in selected_cases.items():
        for n_particles in args.n_particles:
            for backend in backends:
                launcher = choose_launcher(args.launcher, backend)
                tag = f"{case_name}_{backend.name}_N{n_particles}"
                run_dir = runs_dir / case_name / backend.name / f"N{n_particles}"
                run_dir.mkdir(parents=True, exist_ok=True)
                log_path = logs_dir / f"{tag}.log"
                command = build_command(args, backend, launcher, input_path)
                env = build_env(args, mcdc_root, data_library, n_particles)

                print(
                    f"[run] case={case_name:<2} N={n_particles:<8} "
                    f"backend={backend.name:<20} launcher={launcher}"
                )
                print(f"      cmd={shlex.join(command)}")

                if args.dry_run:
                    continue

                t0 = time.perf_counter()
                with log_path.open("w", encoding="utf-8") as log_handle:
                    log_handle.write(f"# command: {shlex.join(command)}\n")
                    log_handle.write(f"# workdir: {run_dir}\n")
                    log_handle.write(f"# case: {case_name}\n")
                    log_handle.write(f"# backend: {backend.name}\n")
                    log_handle.write(f"# n_particles: {n_particles}\n")
                    log_handle.write("\n")
                    completed = subprocess.run(
                        command,
                        cwd=run_dir,
                        env=env,
                        stdout=log_handle,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
                wall_s = time.perf_counter() - t0
                output_path = run_dir / "output.h5"
                output_exists = output_path.exists()
                status = "OK" if completed.returncode == 0 and output_exists else "FAIL"
                print(
                    f"      wall={wall_s:8.2f}s status={status} "
                    f"log={log_path.name}"
                )

                row = {
                    "case": case_name,
                    "backend": backend.name,
                    "mode": backend.mode,
                    "target": backend.target,
                    "launcher": launcher,
                    "ranks": backend.ranks,
                    "n_particles": n_particles,
                    "returncode": completed.returncode,
                    "output_exists": output_exists,
                    "wall_s": f"{wall_s:.4f}",
                    "log_path": str(log_path),
                    "run_dir": str(run_dir),
                    "output_h5": str(output_path),
                    "command": shlex.join(command),
                }
                rows.append(row)

                if status != "OK":
                    failures += 1
                    if args.stop_on_error:
                        write_csv(rows, results_dir / "summary.csv")
                        return 1

    write_csv(rows, results_dir / "summary.csv")

    if args.dry_run:
        print("\nDry run completed.")
        return 0

    print(
        f"\nCompleted {len(rows)} run(s); failures={failures}. "
        f"Summary: {results_dir / 'summary.csv'}"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
