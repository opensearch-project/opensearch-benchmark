from dataclasses import dataclass

from osbenchmark.builder.models.node import Node


@dataclass
class Host:
    """A representation of a host within a cluster"""

    name: str
    ip: str
    metadata: dict
    node_count: int
    node: Node
