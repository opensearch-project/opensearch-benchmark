import shutil
import subprocess

from osbenchmark.builder.executors.executor import Executor
from osbenchmark.exceptions import ExecutorError
from osbenchmark.utils import process


class ShellExecutor(Executor):
    # pylint: disable=arguments-differ
    def execute(self, host, command, output=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=None, detach=False):
        if output:
            return process.run_subprocess_with_output(command)
        else:
            if process.run_subprocess_with_logging(command, stdout=stdout, stderr=stderr, env=env, detach=detach):
                msg = "Command: \"{}\" returned a non-zero exit code".format(command)
                self.logger.error(msg)
                raise ExecutorError(msg)

    def copy(self, host, source, destination):
        shutil.copy(source, destination)
