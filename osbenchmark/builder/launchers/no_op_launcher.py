from osbenchmark.builder.launchers.launcher import Launcher


class NoOpLauncher(Launcher):
    def start(self, host, node_configurations):
        pass

    def stop(self, host, nodes):
        pass
