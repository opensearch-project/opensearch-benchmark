import logging

from osbenchmark.builder.utils.jdk_resolver import JdkResolver
from osbenchmark.exceptions import SystemSetupError


class JavaHomeResolver:
    def __init__(self, executor):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.jdk_resolver = JdkResolver(executor)

    def resolve_java_home(self, host, provision_config_instance):
        is_runtime_jdk_bundled = provision_config_instance.variables["system"]["runtime"]["jdk"]["bundled"]
        runtime_jdks = provision_config_instance.variables["system"]["runtime"]["jdk"]["version"]

        try:
            allowed_runtime_jdks = [int(v) for v in runtime_jdks.split(",")]
        except ValueError:
            raise SystemSetupError(f"ProvisionConfigInstance variable key \"runtime.jdk\" is invalid: \"{runtime_jdks}\" (must be int)")

        if is_runtime_jdk_bundled:
            return self._handle_bundled_jdk(host, allowed_runtime_jdks)
        else:
            self.logger.info("Allowed JDK versions are %s.", allowed_runtime_jdks)
            return self._detect_jdk(host, allowed_runtime_jdks)

    def _handle_bundled_jdk(self, host, allowed_runtime_jdks):
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
