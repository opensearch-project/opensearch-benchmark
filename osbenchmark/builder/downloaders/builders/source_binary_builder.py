import logging
import os

from osbenchmark.builder.downloaders.builders.binary_builder import BinaryBuilder
from osbenchmark.exceptions import ExecutorError, BuildError


class SourceBinaryBuilder(BinaryBuilder):
    def __init__(self, executor, path_manager, jdk_resolver, source_directory, build_jdk_version, log_directory):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.path_manager = path_manager
        self.jdk_resolver = jdk_resolver

        self.source_directory = source_directory
        self.build_jdk_version = build_jdk_version
        self.log_directory = log_directory

    def build(self, host, build_commands, override_source_directory=None):
        for build_command in build_commands:
            self._run_build_command(host, build_command, override_source_directory)

    def _run_build_command(self, host, build_command, override_source_directory):
        source_directory = self.source_directory if override_source_directory is None else override_source_directory

        self.path_manager.create_path(host, self.log_directory, create_locally=False)
        log_file = os.path.join(self.log_directory, "build.log")

        _, jdk_path = self.jdk_resolver.resolve_jdk_path(host, self.build_jdk_version)
        self.executor.execute(host, f"export JAVA_HOME={jdk_path}")

        self.logger.info("Running build command [%s]", build_command)
        try:
            self.executor.execute(host, f"{source_directory}/{build_command} > {log_file} 2>&1")
        except ExecutorError:
            raise BuildError(f"Executing {build_command} failed. The build log can be found at {log_file}")
