from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.repositories.repository_url_provider import RepositoryUrlProvider
from osbenchmark.exceptions import SystemSetupError


class RepositoryUrlProviderTest(TestCase):
    def setUp(self):
        self.template_renderer = Mock()
        self.artifact_variables_provider = Mock()

        self.host = None
        self.variables = {
            "distribution": {
                "version": "1.2.3"
            },
            "fake": {
                "url": "opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz"
            }
        }
        self.url_key = "fake.url"

        self.repo_url_provider = RepositoryUrlProvider(self.template_renderer, self.artifact_variables_provider)

    def test_get_url(self):
        self.artifact_variables_provider.get_artifact_variables.return_value = {"fake": "vars"}

        self.repo_url_provider.render_url_for_key(self.host, self.variables, self.url_key)
        self.artifact_variables_provider.get_artifact_variables.assert_has_calls([
            mock.call(self.host, "1.2.3")
        ])
        self.template_renderer.render_template_string.assert_has_calls([
            mock.call("opensearch/{{VERSION}}/opensearch-{{VERSION}}-{{OSNAME}}-{{ARCH}}.tar.gz", {"fake": "vars"})
        ])

    def test_no_url_template_found(self):
        with self.assertRaises(SystemSetupError):
            self.repo_url_provider.render_url_for_key(self.host, self.variables, "not.real")

    def test_no_url_template_found_not_mandatory(self):
        url = self.repo_url_provider.render_url_for_key(self.host, self.variables, "not.real", False)
        self.assertEqual(url, None)
