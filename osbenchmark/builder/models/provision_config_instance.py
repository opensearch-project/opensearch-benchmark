from dataclasses import dataclass, field
from typing import List

from osbenchmark.builder.models.cluster_flavors import ClusterFlavor
from osbenchmark.builder.models.cluster_infra_providers import ClusterInfraProvider


@dataclass
class ProvisionConfigInstance:
    ENTRY_POINT = "config"

    """
    Creates new settings for a benchmark candidate.

    :param names: Descriptive name(s) for this provision_config_instance.
    :param root_path: The root path from which bootstrap hooks should be loaded if any. May be ``None``.
    :param provider: The infrastructure provider for the cluster
    :param flavor: The flavor of cluster to be provisioned
    :param config_paths: A non-empty list of paths where the raw config can be found.
    :param variables: A dict containing variable definitions that need to be replaced.
    """
    names: List[str]
    root_path: str
    provider: ClusterInfraProvider = ClusterInfraProvider.LOCAL
    flavor: ClusterFlavor = ClusterFlavor.SELF_MANAGED
    config_paths: List[str] = field(default_factory=list)
    variables: dict = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.names, str):
            self.names = [self.names]

    @staticmethod
    def get_entry_point():
        return ProvisionConfigInstance.ENTRY_POINT

    @property
    def name(self):
        return "+".join(self.names)

    # Adapter method for BootstrapHookHandler
    @property
    def config(self):
        return self.name

    @property
    def safe_name(self):
        return "_".join(self.names)

    def __str__(self):
        return self.name
