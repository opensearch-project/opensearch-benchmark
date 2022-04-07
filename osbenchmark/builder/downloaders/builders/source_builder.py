import logging
import os

from osbenchmark.builder.utils.jdk_resolver import JdkResolver
from osbenchmark.builder.utils.path_manager import PathManager
from osbenchmark.exceptions import ExecutorError, BuildError


class SourceBuilder:
    def __init__(self, executor, source_directory, build_jdk, log_directory):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.path_manager = PathManager(executor)
        self.jdk_resolver = JdkResolver(executor)

        self.source_directory = source_directory
        self.build_jdk = build_jdk
        self.log_directory = log_directory

    def build(self, host, build_commands, override_source_directory=None):
        if isinstance(build_commands, str):
            build_commands = [build_commands]

        for build_command in build_commands:
            self._run_build_command(host, build_command, override_source_directory)

    def _run_build_command(self, host, build_command, override_source_directory):
        source_directory = self.source_directory if override_source_directory is None else override_source_directory

        self.path_manager.create_path(host, self.log_directory, create_locally=False)
        log_file = os.path.join(self.log_directory, "build.log")

        jdk_path = self.jdk_resolver.resolve_jdk_path(host, self.build_jdk)
        self.executor.execute(host, f"export JAVA_HOME={jdk_path}")

        self.logger.info("Running build command [%s]", build_command)
        try:
            self.executor.execute(host, f"{source_directory}/{build_command} > {log_file} 2>&1")
        except ExecutorError:
            raise BuildError(f"Executing {build_command} failed. The build log can be found at {log_file}")
