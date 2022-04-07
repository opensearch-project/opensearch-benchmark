import logging
import os

from osbenchmark.utils import io


class ConfigApplier:
    def __init__(self, executor, template_renderer, path_manager):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.template_renderer = template_renderer
        self.path_manager = path_manager

    def apply_configs(self, host, node, config_paths, config_vars):
        mounts = {}
        for config_path in config_paths:
            mounts.update(self._apply_config(host, config_path, node.binary_path, config_vars))

        return mounts

    def _apply_config(self, host, source_root_path, target_root_path, config_vars):
        mounts = {}

        for root, _, files in os.walk(source_root_path):
            relative_root = root[len(source_root_path) + 1:]
            absolute_target_root = os.path.join(target_root_path, relative_root)
            self.path_manager.create_path(host, absolute_target_root)

            for name in files:
                source_file = os.path.join(root, name)
                target_file = os.path.join(absolute_target_root, name)
                mounts[target_file] = os.path.join("/usr/share/opensearch", relative_root, name)

                if io.is_plain_text(source_file):
                    self.logger.info("Reading config template file [%s] and writing to [%s].", source_file, target_file)
                    with open(target_file, mode="a", encoding="utf-8") as f:
                        f.write(self.template_renderer.render_template_file(root, config_vars, source_file))

                    self.executor.execute(host, f"cp {target_file} {target_file}")
                else:
                    self.logger.info("Treating [%s] as binary and copying as is to [%s].", source_file, target_file)
                    self.executor.execute(host, f"cp {source_file} {target_file}")

        return mounts
