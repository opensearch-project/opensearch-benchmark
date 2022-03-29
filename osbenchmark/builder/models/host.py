from dataclasses import dataclass

from osbenchmark.builder.models.node import Node


@dataclass
class Host:
    """A representation of a host within a cluster"""

    name: str
    address: str
    metadata: dict
    node: Node
