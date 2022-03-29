import os

import jinja2
from jinja2 import select_autoescape

from osbenchmark.exceptions import InvalidSyntax, SystemSetupError
from osbenchmark.utils import io, console


class Installer:
    """
    Installers are invoked to prepare the OpenSearch and Plugin data that exists on a host so that an OpenSearch cluster
    can be started.
    """

    def __init__(self, executor, logger):
        self.executor = executor
        self.logger = logger

    def install(self, host, binaries, all_node_ips):
        """
        Executes the necessary logic to prepare and install OpenSearch and any request Plugins on a cluster host

        ;param host: A Host object defining the host on which to install the data
        ;param binaries: A map of components to install to their paths on the host
        ;param all_node_ips: A list of the ips for each node in the cluster. Used for cluster formation
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

    def _apply_configs(self, host, node, config_paths, config_vars):
        mounts = {}
        for config_path in config_paths:
            mounts.update(self._apply_config(host, config_path, node.binary_path, config_vars))

        return mounts

    def _apply_config(self, host, source_root_path, target_root_path, config_vars):
        mounts = {}

        for root, _, files in os.walk(source_root_path):
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(root),
                                     autoescape=select_autoescape(['html', 'xml']))
            relative_root = root[len(source_root_path) + 1:]
            absolute_target_root = os.path.join(target_root_path, relative_root)
            self._create_directory(host, absolute_target_root)

            for name in files:
                source_file = os.path.join(root, name)
                target_file = os.path.join(absolute_target_root, name)
                mounts[target_file] = os.path.join("/usr/share/opensearch", relative_root, name)

                if io.is_plain_text(source_file):
                    self.logger.info("Reading config template file [%s] and writing to [%s].", source_file, target_file)
                    with open(target_file, mode="a", encoding="utf-8") as f:
                        f.write(self._render_template(env, config_vars, source_file))

                    self.executor.execute(host, "cp {0} {0}".format(target_file))
                else:
                    self.logger.info("Treating [%s] as binary and copying as is to [%s].", source_file, target_file)
                    self.executor.execute(host, "cp {} {}".format(source_file, target_file))

        return mounts

    def _create_directory(self, host, directory):
        # Create directory locally and on the host
        io.ensure_dir(directory)
        self.executor.execute(host, "mkdir -m 0777 -p " + directory)

    def _render_template(self, env, variables, file_name):
        try:
            template = env.get_template(io.basename(file_name))
            # force a new line at the end. Jinja seems to remove it.
            return template.render(variables) + "\n"
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise InvalidSyntax("%s in %s" % (str(e), file_name))
        except BaseException as e:
            raise SystemSetupError("%s in %s" % (str(e), file_name))

    def _cleanup(self, host, preserve_install):
        if preserve_install:
            console.info("Preserving benchmark candidate installation.", logger=self.logger)
            return

        self.logger.info("Wiping benchmark candidate installation at [%s].", host.node.binary_path)

        for data_path in host.node.data_paths:
            self._delete_path(host, data_path)

        self._delete_path(host, host.node.binary_path)

    def _delete_path(self, host, path):
        path_block_list = ["", "*", "/", None]
        if path in path_block_list:
            return

        self.executor.execute(host, "rm -r " + path)


