"""
datasources/base.py
===================
The interface every simulation data source must implement.

The replay pipeline (pacing + MQTT publishing) only ever talks to this
interface, so a different backend (e.g. InfluxDB) can be dropped in without
touching the controller, the publisher or the dashboard.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class SimulationDataSource(ABC):
    @abstractmethod
    def get_entities(self) -> Dict[str, List[str]]:
        """entity path -> list of variable names"""

    @abstractmethod
    def get_step_count(self) -> int: ...

    @abstractmethod
    def get_time_axis(self) -> List[float]:
        """t[step] in seconds, one entry per step"""

    @abstractmethod
    def read_step(self, step: int) -> Dict[str, Dict[str, Any]]:
        """entity -> {variable: value} for exactly this step"""

    @abstractmethod
    def get_attributes(self, entity: str, variable: str) -> Dict: ...

    @abstractmethod
    def get_run_metadata(self) -> Dict:
        """arbitrary provenance dict: source identifier, file/query, discovered shape"""
