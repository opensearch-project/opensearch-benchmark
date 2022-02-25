class ClusterBuilder:
    def __init__(self, provisioner, downloader, installer, launcher):
        self.provisioner = provisioner
        self.downloader = downloader
        self.installer = installer
        self.launcher = launcher

    def create_cluster(self):
        raise NotImplementedError

    def delete_cluster(self):
        raise NotImplementedError
