"""
datasources/hdf5_source.py
==========================
`SimulationDataSource` backed by an HDF5 simulation file.

This is the *only* module in the project that imports h5py.  The dataset
discovery / loading / NumPy-to-JSON conversion logic was moved here verbatim
from `hdf5_mqtt_publisher.py` — the file is still read eagerly into memory at
construction time and `read_step()` simply indexes the in-memory arrays.
"""

import logging
from typing import Any, Dict, List, Optional

import h5py

from .base import SimulationDataSource

log = logging.getLogger("hdf5_mqtt_publisher")


# ---------------------------------------------------------------------------
# HDF5 helpers (moved from hdf5_mqtt_publisher.py)
# ---------------------------------------------------------------------------

def _collect_datasets(
    node: h5py.Group,
    path: str = "",
    result: Optional[List[str]] = None,
) -> List[str]:
    """Recursively walk an HDF5 group and collect all dataset paths."""
    if result is None:
        result = []
    for key in node.keys():
        item_path = f"{path}/{key}"
        item = node[key]
        if isinstance(item, h5py.Dataset):
            result.append(item_path)
        elif isinstance(item, h5py.Group):
            _collect_datasets(item, item_path, result)
    return result


def explore_hdf5(file_path: str) -> None:
    """Print the full dataset tree of an HDF5 file (for discovery)."""
    log.info("Exploring HDF5 structure: %s", file_path)
    with h5py.File(file_path, "r") as hf:
        datasets = _collect_datasets(hf)
    if not datasets:
        log.warning("No datasets found in file.")
        return
    print("\n── HDF5 Dataset Tree ──────────────────────────────────────")
    for ds in sorted(datasets):
        print(f"  {ds}")
    print("────────────────────────────────────────────────────────────\n")


def _pick_dataset_interactively(file_path: str) -> str:
    """List all datasets and ask the user to choose one."""
    with h5py.File(file_path, "r") as hf:
        datasets = sorted(_collect_datasets(hf))
    if not datasets:
        raise RuntimeError("No datasets found in the HDF5 file.")

    print("\nAvailable datasets:")
    for i, ds in enumerate(datasets):
        print(f"  [{i}] {ds}")
    while True:
        raw = input("\nEnter index or full dataset path: ").strip()
        if raw.isdigit():
            idx = int(raw)
            if 0 <= idx < len(datasets):
                return datasets[idx]
            print(f"  Index out of range (0–{len(datasets) - 1})")
        elif raw in datasets:
            return raw
        else:
            print("  Not recognised – try again.")


def load_all_datasets(file_path: str) -> Dict[str, Any]:
    """
    Load all datasets from the HDF5 file and return them in a dictionary.
    Keys are the dataset paths, values are tuples of (data_array, attrs_dict).
    """
    datasets = {}
    with h5py.File(file_path, "r") as hf:
        # Only collect from /Series to avoid /Relations which has different shapes
        root = hf["Series"] if "Series" in hf else hf
        paths = _collect_datasets(root)
        for p in paths:
            # Reconstruct the full path
            full_path = p if "Series" not in hf else f"/Series/{p.lstrip('/')}"
            try:
                ds = hf[full_path]
            except KeyError:
                continue

            data = ds[()]
            if data.ndim > 0:  # Only load time-series data
                attrs = {k: _value_to_python(v) for k, v in ds.attrs.items()}
                datasets[full_path] = (data, attrs)
    log.info("Loaded %d time-series datasets from '%s'", len(datasets), file_path)
    return datasets


def _value_to_python(val: Any) -> Any:
    """Convert NumPy scalars / arrays to JSON-serialisable Python types."""
    if hasattr(val, "item"):
        try:
            val = val.item()
        except ValueError:
            pass # multi-element arrays might fail item()

    if hasattr(val, "tolist"):
        val = val.tolist()

    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    elif isinstance(val, list):
        return [_value_to_python(v) for v in val]
    elif isinstance(val, dict):
        return {k: _value_to_python(v) for k, v in val.items()}
    return val


def _split_dataset_path(full_path: str) -> tuple:
    """'/Series/<entity>/<variable>' -> ('<entity>', '<variable>')."""
    clean = full_path.lstrip("/")
    if clean.startswith("Series/"):
        clean = clean[7:]
    entity, _, variable = clean.partition("/")
    return entity, variable


# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------

class HDF5DataSource(SimulationDataSource):
    """Eagerly loads every 1-D time-series dataset under `/Series` into memory."""

    #: entity/variable used for the simulation time axis, when present
    TIME_ENTITY = "time"
    TIME_VARIABLE = "t"

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        # dataset full path -> (numpy array, attrs).  Insertion order is the
        # HDF5 walk order and defines the publish order downstream.
        self._datasets: Dict[str, Any] = load_all_datasets(file_path)

        self._entities: Dict[str, List[str]] = {}
        self._by_entity: Dict[str, Dict[str, Any]] = {}
        for full_path, (data, attrs) in self._datasets.items():
            entity, variable = _split_dataset_path(full_path)
            self._entities.setdefault(entity, []).append(variable)
            self._by_entity.setdefault(entity, {})[variable] = (data, attrs)

        lengths = [data.shape[0] for data, _ in self._datasets.values()]
        self._n_steps = min(lengths) if lengths else 0

    # -- interface ----------------------------------------------------------

    def get_entities(self) -> Dict[str, List[str]]:
        return {entity: list(variables) for entity, variables in self._entities.items()}

    def get_step_count(self) -> int:
        return self._n_steps

    def get_time_axis(self) -> List[float]:
        series = self._by_entity.get(self.TIME_ENTITY, {}).get(self.TIME_VARIABLE)
        if series is not None:
            data, _ = series
            return [float(v) for v in data[: self._n_steps]]
        return [float(i) for i in range(self._n_steps)]

    def read_step(self, step: int) -> Dict[str, Dict[str, Any]]:
        return {
            entity: {
                variable: _value_to_python(data[step])
                for variable, (data, _) in variables.items()
            }
            for entity, variables in self._by_entity.items()
        }

    def get_attributes(self, entity: str, variable: str) -> Dict:
        series = self._by_entity.get(entity, {}).get(variable)
        return series[1] if series else {}

    def get_run_metadata(self) -> Dict:
        return {
            "source_type": "hdf5",
            "source": self.file_path,
            "n_datasets": len(self._datasets),
            "n_entities": len(self._entities),
        }

    # -- convenience --------------------------------------------------------

    def is_empty(self) -> bool:
        return not self._datasets
