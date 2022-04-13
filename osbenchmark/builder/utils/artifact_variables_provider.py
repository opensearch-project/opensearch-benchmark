from osbenchmark.builder.models.architecture_types import ArchitectureTypes


class ArtifactVariablesProvider:
    def __init__(self, executor):
        self.executor = executor

    def get_artifact_variables(self, host, opensearch_version=None):
        return {
            "VERSION": opensearch_version,
            "OSNAME": self._get_os_name(host),
            "ARCH": self._get_arch(host)
        }

    def _get_os_name(self, host):
        os_name = self.executor.execute(host, "uname", output=True)[0]
        return os_name.lower()

    def _get_arch(self, host):
        arch = self.executor.execute(host, "uname -m", output=True)[0]
        return ArchitectureTypes.get_from_hardware_name(arch.lower()).opensearch_name
