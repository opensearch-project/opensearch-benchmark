import jinja2

from osbenchmark.exceptions import InvalidSyntax, SystemSetupError
from osbenchmark.utils import io


class Installer:
    """
    Installers are invoked to prepare the OpenSearch and Plugin data that exists on a host so that an OpenSearch cluster
    can be started.
    """

    def __init__(self, executor):
        self.executor = executor

    def install(self, host, binaries):
        """
        Executes the necessary logic to prepare and install OpenSearch and any request Plugins on a cluster host

        ;param host: A Host object defining the host on which to install the data
        ;param binaries: A map of components to install to their paths on the host
        ;return node: A Node object detailing the installation data of the node on the host
        """
        raise NotImplementedError

    def cleanup(self, host):
        """
        Removes the data that was downloaded, installed, and created on a given host during the test execution

        ;param host: A Host object defining the host on which to remove the data
        ;return None
        """
        raise NotImplementedError

    def _render_template(self, env, variables, file_name):
        try:
            template = env.get_template(io.basename(file_name))
            # force a new line at the end. Jinja seems to remove it.
            return template.render(variables) + "\n"
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise InvalidSyntax("%s in %s" % (str(e), file_name))
        except BaseException as e:
            raise SystemSetupError("%s in %s" % (str(e), file_name))
