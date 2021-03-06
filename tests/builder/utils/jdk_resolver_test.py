from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.builder.utils.jdk_resolver import JdkResolver
from osbenchmark.exceptions import SystemSetupError


class JdkResolverTests(TestCase):
    def setUp(self):
        self.host = None
        self.executor = Mock()
        self.jdk_resolver = JdkResolver(self.executor)

    def test_success_pre_java_9(self):
        # printenv, $JAVA_HOME -XshowSettings:properties -version
        self.executor.execute.side_effect = [["JAVA7_HOME=/fake/path"], ["java.vm.specification.version = 1.7.0"]]

        _, jdk_path = self.jdk_resolver.resolve_jdk_path(self.host, 7)
        self.assertEqual("/fake/path", jdk_path)

    def test_success_post_java_8(self):
        # printenv, $JAVA_HOME -XshowSettings:properties -version
        self.executor.execute.side_effect = [["JAVA9_HOME=/fake/path"], ["java.vm.specification.version = 9"]]

        _, jdk_path = self.jdk_resolver.resolve_jdk_path(self.host, 9)
        self.assertEqual("/fake/path", jdk_path)

    def test_generic_java_home_matches(self):
        # printenv, $JAVA_HOME -XshowSettings:properties -version
        self.executor.execute.side_effect = [["JAVA_HOME=/fake/path"], ["java.vm.specification.version = 9"]]

        _, jdk_path = self.jdk_resolver.resolve_jdk_path(self.host, 9)
        self.assertEqual("/fake/path", jdk_path)

    def test_multiple_majors(self):
        # printenv, $JAVA_HOME -XshowSettings:properties -version x 2
        self.executor.execute.side_effect = [
            ["JAVA_HOME=/fake/path", "JAVA14_HOME=/another/fake/path"], ["java.vm.specification.version = 14"],
            ["java.vm.specification.version = 9"]
        ]

        _, jdk_path = self.jdk_resolver.resolve_jdk_path(self.host, [8, 14, 16])
        self.assertEqual("/another/fake/path", jdk_path)

    def test_no_matching_version(self):
        # printenv, $JAVA_HOME -XshowSettings:properties -version
        self.executor.execute.side_effect = [["JAVA_HOME=/fake/path"], ["java.vm.specification.version = 9"]]

        with self.assertRaises(SystemSetupError):
            self.jdk_resolver.resolve_jdk_path(self.host, 10)

    def test_no_java_home_set(self):
        # printenv, $JAVA_HOME -XshowSettings:properties -version
        self.executor.execute.side_effect = [[]]

        with self.assertRaises(SystemSetupError):
            self.jdk_resolver.resolve_jdk_path(self.host, 10)

    def test_version_does_not_match_env_var_name(self):
        # printenv, $JAVA_HOME -XshowSettings:properties -version
        self.executor.execute.side_effect = [["JAVA8_HOME=/fake/path"], ["java.vm.specification.version = 9"]]

        with self.assertRaises(SystemSetupError):
            self.jdk_resolver.resolve_jdk_path(self.host, 8)
