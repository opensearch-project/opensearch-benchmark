from osbenchmark.builder.executors.executor import Executor
from osbenchmark.exceptions import ExecutorError


class ExceptionHandlingExecutor(Executor):
    def __init__(self, executor):
        super().__init__()
        self.executor = executor

    def execute(self, host, command, **kwargs):
        try:
            self.executor.execute(host, command, kwargs)
        except Exception as e:
            raise ExecutorError("Command \"{}\" on host \"{}\" failed to execute".format(command, host), e)

    def copy(self, host, source, destination):
        try:
            self.executor.copy(host, source, destination)
        except Exception as e:
            raise ExecutorError("Copying \"{}\" to \"{}\" failed on host \"{}\"".format(source, destination, host), e)
