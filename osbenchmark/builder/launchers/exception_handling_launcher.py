from osbenchmark.builder.launchers.launcher import Launcher
from osbenchmark.exceptions import LaunchError


class ExceptionHandlingLauncher(Launcher):
    def __init__(self, launcher, shell_executor=None):
        super().__init__(shell_executor)
        self.launcher = launcher

    def start(self, host, node_configurations):
        try:
            return self.launcher.start(host, node_configurations)
        except Exception as e:
            raise LaunchError(f"Starting node(s) on host \"{host}\" failed", e)

    def stop(self, host, nodes):
        try:
            return self.launcher.stop(host, nodes)
        except Exception as e:
            raise LaunchError(f"Stopping node(s) on host \"{host}\" failed", e)
