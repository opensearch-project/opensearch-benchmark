from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.builder.utils.artifact_variables_provider import ArtifactVariablesProvider


class ArtifactVariablesProviderTest(TestCase):
    def setUp(self):
        self.host = None

        self.executor = Mock()
        self.artifact_variables_provider = ArtifactVariablesProvider(self.executor)

    def test_x86(self):
        self.executor.execute.side_effect = [["Linux"], ["x86_64"]]
        variables = self.artifact_variables_provider.get_artifact_variables(self.host)

        self.assertEqual(variables, {
            "VERSION": None,
            "OSNAME": "linux",
            "ARCH": "x64"
        })

    def test_arm(self):
        self.executor.execute.side_effect = [["Linux"], ["aarch64"]]
        variables = self.artifact_variables_provider.get_artifact_variables(self.host)

        self.assertEqual(variables, {
            "VERSION": None,
            "OSNAME": "linux",
            "ARCH": "arm64"
        })

    def test_version_supplied(self):
        self.executor.execute.side_effect = [["Linux"], ["aarch64"]]
        variables = self.artifact_variables_provider.get_artifact_variables(self.host, "1.23")

        self.assertEqual(variables, {
            "VERSION": "1.23",
            "OSNAME": "linux",
            "ARCH": "arm64"
        })
