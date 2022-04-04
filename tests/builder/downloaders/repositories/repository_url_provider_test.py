from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.builder.downloaders.repositories.repository_url_provider import RepositoryUrlProvider
from osbenchmark.exceptions import SystemSetupError


class RepositoryUrlProviderTest(TestCase):
    def setUp(self):
        self.executor = Mock()

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

        self.repo_url_provider = RepositoryUrlProvider(self.executor)

    def test_get_url_aarch64(self):
        self.executor.execute.side_effect = [["Linux"], ["aarch64"]]

        url = self.repo_url_provider.render_url_for_key(self.host, self.variables, self.url_key)
        self.assertEqual(url, "opensearch/1.2.3/opensearch-1.2.3-linux-arm64.tar.gz")

    def test_get_url_x86(self):
        self.executor.execute.side_effect = [["Linux"], ["x86_64"]]

        url = self.repo_url_provider.render_url_for_key(self.host, self.variables, self.url_key)
        self.assertEqual(url, "opensearch/1.2.3/opensearch-1.2.3-linux-x64.tar.gz")

    def test_no_url_template_found(self):
        with self.assertRaises(SystemSetupError):
            self.repo_url_provider.render_url_for_key(self.host, self.variables, "not.real")

    def test_no_url_template_found_not_mandatory(self):
        url = self.repo_url_provider.render_url_for_key(self.host, self.variables, "not.real", False)
        self.assertEqual(url, None)
