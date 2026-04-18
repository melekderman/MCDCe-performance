from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cases.common import ElectronCase, run_case


run_case(
    ElectronCase(
        case_name="Al-1MeV-th0",
        material_symbol="Al",
        energy_eV=1.0e6,
        csda_range_g_cm2=0.569,
        rho_g_cm3=2.70,
        atomic_weight_g_mol=26.7497084,
        areal_density_g_cm2=5.05e-3,
    )
)
