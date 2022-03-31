from dataclasses import dataclass
from typing import List

from osbenchmark.telemetry import Telemetry


@dataclass
class Node:
    """A representation of a node within a host"""

    name: str
    port: int
    pid: int
    root_dir: str
    binary_path: str
    log_path: str
    heap_dump_path: str
    data_paths: List[str]
    telemetry: Telemetry
