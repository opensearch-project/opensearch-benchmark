import logging

from osbenchmark.builder.utils.jdk_resolver import JdkResolver
from osbenchmark.exceptions import SystemSetupError


class JavaHomeResolver:
    def __init__(self, executor):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.jdk_resolver = JdkResolver(executor)

    def resolve_java_home(self, host, provision_config_instance_runtime_jdks, specified_runtime_jdk=None,
                          provides_bundled_jdk=False):
        try:
            allowed_runtime_jdks = [int(v) for v in provision_config_instance_runtime_jdks.split(",")]
        except ValueError:
            raise SystemSetupError("ProvisionConfigInstance variable key \"runtime.jdk\" is invalid: \"{}\" (must be int)"
                                   .format(provision_config_instance_runtime_jdks))

        runtime_jdk_versions = self._determine_runtime_jdks(specified_runtime_jdk, allowed_runtime_jdks)

        if runtime_jdk_versions[0] == "bundled":
            return self._handle_bundled_jdk(host, allowed_runtime_jdks, provides_bundled_jdk)
        else:
            self.logger.info("Allowed JDK versions are %s.", runtime_jdk_versions)
            return self._detect_jdk(host, runtime_jdk_versions)

    def _determine_runtime_jdks(self, specified_runtime_jdk, allowed_runtime_jdks):
        if specified_runtime_jdk:
            return [specified_runtime_jdk]
        else:
            return allowed_runtime_jdks

    def _handle_bundled_jdk(self, host, allowed_runtime_jdks, provides_bundled_jdk):
        if not provides_bundled_jdk:
            raise SystemSetupError(
                "This OpenSearch version does not contain a bundled JDK. Please specify a different runtime JDK.")

        self.logger.info("Using JDK bundled with OpenSearch.")
        os_check = self.executor.execute(host, "uname", output=True)[0]
        if os_check == "Windows":
            raise SystemSetupError("OpenSearch doesn't provide release artifacts for Windows currently.")
        if os_check == "Darwin":
            # OpenSearch does not provide a Darwin version of OpenSearch or a MacOS JDK version
            self.logger.info("Using JDK set from JAVA_HOME because OS is MacOS (Darwin).")
            self.logger.info(
                "NOTICE: OpenSearch doesn't provide jdk bundled release artifacts for MacOS (Darwin) currently. "
                "Please set JAVA_HOME to JDK 11 or JDK 8 and set the runtime.jdk.bundled to true in the specified "
                "provision config instance file")
            return self._detect_jdk(host, allowed_runtime_jdks)

        # assume that the bundled JDK is the highest available; the path is irrelevant
        return allowed_runtime_jdks[0], None

    def _detect_jdk(self, host, jdks):
        major, java_home = self.jdk_resolver.resolve_jdk_path(host, jdks)
        self.logger.info("Detected JDK with major version [%s] in [%s].", major, java_home)
        return major, java_home
