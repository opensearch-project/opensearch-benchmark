"""
Provisioners are used to create and destroy any infrastructure required to construct a cluster.
"""
class Provisioner:
    def __init__(self):
        pass

    """
    Provisions the necessary infrastructure for creating a cluster
    
    ;return hosts: A list of Host objects defining the hosts in a cluster
    """
    def provision_infrastructure(self):
        raise NotImplementedError

    """
    Destroys the infrastructure created for a given cluster
    
    ;param cluster: A Cluster object representing the cluster to be torn down
    ;return None
    """
    def tear_down_infrastructure(self, cluster):
        raise NotImplementedError
