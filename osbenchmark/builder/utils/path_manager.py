from osbenchmark.utils import io


class PathManager:
    def __init__(self, executor):
        self.executor = executor

    def create_path(self, host, path, create_locally=True):
        if create_locally:
            io.ensure_dir(path)
        self.executor.execute(host, "mkdir -m 0777 -p " + path)

    def delete_path(self, host, path):
        path_block_list = ["", "*", "/", None]
        if path in path_block_list:
            return

        self.executor.execute(host, "rm -r " + path)
