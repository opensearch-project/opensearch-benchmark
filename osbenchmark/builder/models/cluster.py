from dataclasses import dataclass
from typing import List

from osbenchmark.builder.models.host import Host


@dataclass
class Cluster:
    """A representation of the cluster used in the benchmark"""

    name: str
    hosts: List[Host]
