from dataclasses import dataclass
from typing import List

from osbenchmark.telemetry import Telemetry


@dataclass
class Node:
    name: str
    port: int
    pid: int
    root_dir: str
    binary_path: str
    data_paths: List[str]
    telemetry: Telemetry
