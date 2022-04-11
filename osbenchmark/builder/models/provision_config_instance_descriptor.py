from dataclasses import dataclass, field
from typing import List

from osbenchmark.builder.models.cluster_flavors import ClusterFlavor
from osbenchmark.builder.models.cluster_infra_providers import ClusterInfraProvider
from osbenchmark.builder.models.provision_config_instance_types import ProvisionConfigInstanceType


@dataclass
class ProvisionConfigInstanceDescriptor:
    """
    A ProvisionConfigInstanceDescriptor represents a single source of provision config definition. These descriptors serve
    as an intermediary store of the cluster to be provisioned. Descriptors are created from each config source and played
    on top of one another to create the final ProvisionConfigInstance to be used by the Builder system.

    :param name: Descriptive name for this provision config instance source.
    :param description: A description for this provision config instance source.
    :param type: The type of provision config instance source. Can be a standalone config instance or a mixin
    :param root_paths: A list of root paths from which bootstrap hooks should be loaded if any. May be empty.
    :param provider: The infrastructure provider for the cluster. May be ``None``.
    :param flavor: The flavor of cluster to be provisioned. May be ``None``.
    :param config_paths: A list of paths where the raw config can be found. May be empty.
    :param variables: A dict containing variable definitions that need to be replaced.
    """

    name: str
    description: str = ""
    type: ProvisionConfigInstanceType = ProvisionConfigInstanceType.PROVISION_CONFIG_INSTANCE
    root_paths: List[str] = field(default_factory=list)
    provider: ClusterInfraProvider = None
    flavor: ClusterFlavor = None
    config_paths: List[str] = field(default_factory=list)
    variables: dict = field(default_factory=dict)
