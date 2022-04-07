import os
import re

from osbenchmark.exceptions import SystemSetupError
from osbenchmark.utils import io


class JdkResolver:
    SYS_PROP_REGEX = r".*%s.*=\s?(.*)"

    def __init__(self, executor):
        self.executor = executor

    def resolve_jdk_path(self, host, majors):
        """
        Resolves the path to the JDK with the provided major version(s). It checks the versions in the same order specified in ``majors``
        and will return the first match. To achieve this, it first checks the major version x in the environment variable ``JAVAx_HOME``
        and falls back to ``JAVA_HOME``. It also ensures that the environment variable points to the right JDK version.

        If no appropriate version is found, a ``SystemSetupError`` is raised.

        :param host: The host on which to resolve the JDK path
        :param majors: Either a list of major versions to check or a single version as an ``int``.
        :return: A tuple of (major version, path to Java home directory).
        """
        if isinstance(majors, int):
            return majors, self._resolve_jdk_path(host, [majors])
        else:
            return majors, self._resolve_jdk_path(host, majors)

    def _resolve_jdk_path(self, host, majors):
        """
        Resolves the path to a JDK with one of the provided major versions.

        :param majors: The major versions to check.
        :return: The resolved path to the JDK
        """

        defined_env_vars = self._get_defined_env_vars(host)
        java_home_env_var_names = [f"JAVA{major}_HOME" for major in majors]
        java_home_env_var_names.append("JAVA_HOME")

        resolved_major_to_java_home_path = {}
        for java_home_env_var_name in java_home_env_var_names:
            if java_home_env_var_name in defined_env_vars:
                major_to_java_home_path = self._resolve_major_from_java_home(host, java_home_env_var_name,
                                                                             defined_env_vars[java_home_env_var_name])
                if major_to_java_home_path:
                    resolved_major_to_java_home_path.update(major_to_java_home_path)

        for major in majors:
            if major in resolved_major_to_java_home_path:
                return resolved_major_to_java_home_path[major]

        checked_env_vars = self._checked_env_vars(majors)
        raise SystemSetupError(f"Install a JDK with one of the versions {majors} and point to it with one of {checked_env_vars}.")

    def _get_defined_env_vars(self, host):
        env_vars_as_strings = self.executor.execute(host, "printenv", output=True)
        return dict(env_var_as_string.split("=") for env_var_as_string in env_vars_as_strings)

    def _resolve_major_from_java_home(self, host, java_home_env_var_name, java_home_env_var_value):
        if java_home_env_var_value:
            major_version = self._major_version(host, java_home_env_var_value)
            if java_home_env_var_name in ("JAVA_HOME", f"JAVA{major_version}_HOME"):
                return {major_version: java_home_env_var_value}

    def _major_version(self, host, java_home):
        """
        Determines the major version number of JDK available at the provided JAVA_HOME directory.

        :param java_home: The JAVA_HOME directory to check.
        :return: An int, representing the major version number of the JDK available at ``java_home``.
        """
        version = self._system_property(host, java_home, "java.vm.specification.version")
        # are we under the "old" (pre Java 9) or the new (Java 9+) version scheme?
        if version.startswith("1."):
            return int(version[2])
        else:
            return int(version)

    def _system_property(self, host, java_home, system_property_name):
        lines = self.executor.execute(host, f"{self._java(java_home)} -XshowSettings:properties -version", output=True)
        # matches e.g. "    java.runtime.version = 1.8.0_121-b13" and captures "1.8.0_121-b13"
        sys_prop_pattern = re.compile(JdkResolver.SYS_PROP_REGEX % system_property_name)
        for line in lines:
            m = sys_prop_pattern.match(line)
            if m:
                return m.group(1)

        return None

    def _java(self, java_home):
        return io.escape_path(os.path.join(java_home, "bin", "java"))

    def _checked_env_vars(self, majors):
        """
        Provides a list of environment variables that are checked for the given list of major versions.

        :param majors: A list of major versions.
        :return: A list of checked environment variables.
        """
        checked = [f"JAVA{major}_HOME" for major in majors]
        checked.append("JAVA_HOME")
        return checked
