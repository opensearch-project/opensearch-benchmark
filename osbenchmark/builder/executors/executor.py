"""
Executors are used to run shell commands on the cluster hosts. Implementations of this class will use various
technologies to interface with the hosts of a cluster.
"""
class Executor:
    def __init__(self):
        pass

    """
    Executes a list of commands against the provided host
    
    ;param host: A Host object defining the host on which to execute the commands
    ;param commands: A list of strings defining shell commands
    ;return return_code: The return code of the execution
    """
    def execute(self, host, commands):
        raise NotImplementedError
