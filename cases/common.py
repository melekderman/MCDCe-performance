from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import mcdc
import numpy as np


PROCESS_DATA_LIBRARY_ENV = "MCDC_VV_PROCESS_DATA_LIBRARY_DIR"
N_PARTICLES_ENV = "MCDC_ELECTRON_N_PARTICLES"
OUTPUT_NAME_ENV = "MCDC_ELECTRON_OUTPUT_NAME"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_LIBRARY = REPO_ROOT / "electron-vv-data"
AVOGADRO_NUMBER = 6.02214076e23
TINY = 1.0e-30


@dataclass(frozen=True)
class ElectronCase:
    case_name: str
    material_symbol: str
    energy_eV: float
    csda_range_g_cm2: float
    rho_g_cm3: float
    atomic_weight_g_mol: float
    areal_density_g_cm2: float
    angle_deg: float = 0.0
    default_n_particles: int = 10_000


def _get_n_particles(default_value: int) -> int:
    value = os.environ.get(N_PARTICLES_ENV)
    return int(value) if value else default_value


def _get_output_name(case: ElectronCase, n_particles: int) -> str:
    output_name = os.environ.get(OUTPUT_NAME_ENV)
    if output_name:
        return output_name
    e_name = f"{case.energy_eV:.2g}"
    np_name = len(str(n_particles)) - 1
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"lw_{case.material_symbol}_{e_name}eV_1e{np_name}p_{stamp}"


def configure_data_library() -> Path:
    data_library = Path(
        os.environ.get(PROCESS_DATA_LIBRARY_ENV, str(DEFAULT_DATA_LIBRARY))
    ).expanduser()
    os.environ.setdefault(PROCESS_DATA_LIBRARY_ENV, str(data_library))
    os.environ["MCDC_LIB"] = str(data_library)
    return data_library


def run_case(case: ElectronCase) -> None:
    data_library = configure_data_library()
    n_particles = _get_n_particles(case.default_n_particles)
    output_name = _get_output_name(case, n_particles)

    dz = case.areal_density_g_cm2 / case.rho_g_cm3
    material_density = (
        AVOGADRO_NUMBER / case.atomic_weight_g_mol * case.rho_g_cm3 / 1.0e24
    )
    thickness = case.csda_range_g_cm2 / case.rho_g_cm3
    n_layers = max(1, int(thickness / dz))
    theta = math.radians(case.angle_deg)

    print(f"[INFO] case            = {case.case_name}")
    print(f"[INFO] material        = {case.material_symbol}")
    print(f"[INFO] data library    = {data_library}")
    print(f"[INFO] N_PARTICLES     = {n_particles}")
    print(f"[INFO] output          = {output_name}")
    print(f"[INFO] layer thickness = {dz:.6e} cm")
    print(f"[INFO] total thickness = {thickness:.6e} cm")
    print(f"[INFO] N_layers        = {n_layers}")

    material = mcdc.Material(
        element_composition={case.material_symbol: material_density}
    )

    surface_in = mcdc.Surface.PlaneZ(z=0.0, boundary_condition="vacuum")
    surface_out = mcdc.Surface.PlaneZ(z=thickness, boundary_condition="vacuum")
    mcdc.Cell(region=+surface_in & -surface_out, fill=material)

    mcdc.Source(
        z=[TINY, TINY],
        particle_type="electron",
        energy=np.array([[case.energy_eV - 1.0, case.energy_eV + 1.0], [0.5, 0.5]]),
        direction=[math.sin(theta), TINY, math.cos(theta)],
    )

    z_bins = np.linspace(0.0, thickness, n_layers + 1)
    mesh = mcdc.MeshStructured(z=z_bins)
    mcdc.Tally(name="edep", mesh=mesh, scores=["energy_deposition"])
    mcdc.Tally(name="flux", mesh=mesh, scores=["flux"])
    mcdc.Tally(name="s1_current", surface=surface_in, scores=["net-current"])
    mcdc.Tally(name="s2_current", surface=surface_out, scores=["net-current"])

    mcdc.settings.set_transported_particles(["electron"])
    mcdc.settings.N_particle = n_particles
    mcdc.settings.active_bank_buffer = max(10_000, n_particles * 100)
    mcdc.settings.output_name = output_name
    mcdc.settings.use_progress_bar = True

    mcdc.run()
