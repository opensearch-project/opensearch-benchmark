"""
Installers are invoked to prepare the OpenSearch and Plugin data that exists on a host so that an OpenSearch cluster
can be started.
"""
class Installer:
    def __init__(self, executor):
        self.executor = executor

    """
    Executes the necessary logic to prepare and install OpenSearch and any request Plugins on a cluster host
    
    ;param host: A Host object defining the host on which to install the data
    ;param binaries: A map of components to install to their paths on the host
    ;return node_configurations: A list of NodeConfiguration objects detailing the installation data of the nodes on the host
    """
    def install(self, host, binaries):
        raise NotImplementedError

    """
    Removes the data that was downloaded, installed, and created on a given host during the test execution
    
    ;param host: A Host object defining the host on which to remove the data
    ;param node_configurations: A list of NodeConfiguration objects detailing the installation data of the nodes on the host
    ;return None
    """
    def cleanup(self, host, node_configurations):
        raise NotImplementedError
