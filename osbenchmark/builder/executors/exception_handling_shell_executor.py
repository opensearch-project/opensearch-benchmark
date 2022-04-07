from osbenchmark.builder.executors.shell_executor import ShellExecutor
from osbenchmark.exceptions import ExecutorError


class ExceptionHandlingShellExecutor(ShellExecutor):
    def __init__(self, executor):
        self.executor = executor

    def execute(self, host, command, **kwargs):
        try:
            return self.executor.execute(host, command, kwargs)
        except Exception as e:
            raise ExecutorError(f"Command \"{command}\" on host \"{host}\" failed to execute", e)
