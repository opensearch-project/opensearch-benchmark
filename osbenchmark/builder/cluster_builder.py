"""
The ClusterBuilder is the interface into the builder system from the Dispatcher. This class orchestrates all of the
builder subcomponents used to create and delete a cluster.
"""
class ClusterBuilder:
    def __init__(self, provisioner, downloader, installer, launcher):
        self.provisioner = provisioner
        self.downloader = downloader
        self.installer = installer
        self.launcher = launcher

    """
    Creates a cluster using the builder subcomponents
    
    ;return cluster: A Cluster object defining the cluster that was created
    """
    def create_cluster(self):
        raise NotImplementedError

    """
    Deletes a cluster using the builder subcomponents
    
    ;param cluster: A Cluster object defining the cluster to be deleted
    ;return None
    """
    def delete_cluster(self, cluster):
        raise NotImplementedError
