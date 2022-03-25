from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.builder.utils.java_home_resolver import JavaHomeResolver
from osbenchmark.exceptions import SystemSetupError


class JavaHomeResolverTests(TestCase):
    def setUp(self):
        self.host = None
        self.executor = Mock()
        self.java_home_resolver = JavaHomeResolver(self.executor)
        self.java_home_resolver.jdk_resolver = Mock()

    def test_resolves_java_home_for_default_runtime_jdk(self):
        self.executor.execute.return_value = ["Darwin"]
        self.java_home_resolver.jdk_resolver.resolve_jdk_path.return_value = (12, "/opt/jdk12")
        major, java_home = self.java_home_resolver.resolve_java_home(self.host, "12,11,10,9,8", specified_runtime_jdk=None,
                                                                     provides_bundled_jdk=True)

        self.assertEqual(major, 12)
        self.assertEqual(java_home, "/opt/jdk12")

    def test_resolves_java_home_for_specific_runtime_jdk(self):
        self.executor.execute.return_value = ["Darwin"]
        self.java_home_resolver.jdk_resolver.resolve_jdk_path.return_value = (8, "/opt/jdk8")
        major, java_home = self.java_home_resolver.resolve_java_home(self.host, "12,11,10,9,8", specified_runtime_jdk=8,
                                                                     provides_bundled_jdk=True)

        self.assertEqual(major, 8)
        self.assertEqual(java_home, "/opt/jdk8")
        self.java_home_resolver.jdk_resolver.resolve_jdk_path.assert_called_with(None, [8])

    def test_resolves_java_home_for_bundled_jdk_on_linux(self):
        self.executor.execute.return_value = ["Linux"]
        major, java_home = self.java_home_resolver.resolve_java_home(self.host, "12,11,10,9,8", specified_runtime_jdk="bundled",
                                                                     provides_bundled_jdk=True)

        self.assertEqual(major, 12)
        self.assertEqual(java_home, None)

    def test_resolves_java_home_for_bundled_jdk_windows(self):
        self.executor.execute.return_value = ["Windows"]
        with self.assertRaises(SystemSetupError) as ctx:
            self.java_home_resolver.resolve_java_home(self.host, "12,11,10,9", specified_runtime_jdk="bundled", provides_bundled_jdk=True)
        self.assertEqual("OpenSearch doesn't provide release artifacts for Windows currently.", ctx.exception.args[0])

    def test_disallowed_bundled_jdk(self):
        with self.assertRaises(SystemSetupError) as ctx:
            self.java_home_resolver.resolve_java_home(self.host, "12,11,10,9,8", specified_runtime_jdk="bundled")
        self.assertEqual(
            "This OpenSearch version does not contain a bundled JDK. Please specify a different runtime JDK.", ctx.exception.args[0])
