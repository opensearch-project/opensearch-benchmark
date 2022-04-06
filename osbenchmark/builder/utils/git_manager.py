class GitManager:
    def __init__(self, executor):
        self.executor = executor

    def clone(self, host, remote_url, target_dir):
        self.executor.execute(host, f"git clone {remote_url} {target_dir}")

    def fetch(self, host, target_dir, remote="origin"):
        self.executor.execute(host, f"git -C {target_dir} fetch --prune --tags {remote}")

    def checkout(self, host, target_dir, branch="main"):
        self.executor.execute(host, f"git -C {target_dir} checkout {branch}")

    def rebase(self, host, target_dir, remote="origin", branch="main"):
        self.executor.execute(host, f"git -C {target_dir} rebase {remote}/{branch}")

    def get_revision_from_timestamp(self, host, target_dir, timestamp):
        get_revision_from_timestamp_command = f"git -C {target_dir} rev-list -n 1 --before=\"{timestamp}\" --date=iso8601 origin/main"

        return self.executor.execute(host, get_revision_from_timestamp_command, output=True)[0].strip()

    def get_revision_from_local_repository(self, host, target_dir):
        return self.executor.execute(host, f"git -C {target_dir} rev-parse --short HEAD", output=True)[0].strip()
