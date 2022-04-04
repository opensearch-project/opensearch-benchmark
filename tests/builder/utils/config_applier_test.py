from unittest import TestCase, mock
from unittest.mock import Mock, patch, mock_open

from osbenchmark.builder.models.node import Node
from osbenchmark.builder.utils.config_applier import ConfigApplier


class ConfigApplierTest(TestCase):
    def setUp(self):
        self.node = Node(binary_path="/fake_binary_path", data_paths=["/fake1", "/fake2"],
                         name=None, pid=None, telemetry=None, port=None, root_dir=None, log_path=None, heap_dump_path=None)
        self.host = None
        self.config_paths = ["/fake_config_path"]
        self.config_vars = {}

        self.executor = Mock()
        self.template_renderer = Mock()
        self.path_manager = Mock()
        self.config_applier = ConfigApplier(self.executor, self.template_renderer, self.path_manager)

    @mock.patch("os.walk")
    @mock.patch("osbenchmark.utils.io.is_plain_text")
    def test_apply_config_binary_file(self, is_plain_text, os_walk):
        is_plain_text.return_value = False
        os_walk.return_value = [("/fake_config_path/sub_fake_config_path", "fake_something", ["fake_file"])]

        mounts = self.config_applier.apply_configs(self.host, self.node, self.config_paths, self.config_vars)

        self.assertEqual(mounts, {
            "/fake_binary_path/sub_fake_config_path/fake_file": "/usr/share/opensearch/sub_fake_config_path/fake_file"
        })
        self.path_manager.create_path.assert_has_calls([
            mock.call(self.host, "/fake_binary_path/sub_fake_config_path")
        ])
        self.template_renderer.render_template_file.assert_has_calls([])
        self.executor.execute.assert_has_calls([
            mock.call(self.host, "cp /fake_config_path/sub_fake_config_path/fake_file /fake_binary_path/sub_fake_config_path/fake_file")
        ])

    @mock.patch("os.walk")
    @mock.patch("osbenchmark.utils.io.is_plain_text")
    def test_apply_config_plaintext_file(self, is_plain_text, os_walk):
        is_plain_text.return_value = True
        os_walk.return_value = [("/fake_config_path/sub_fake_config_path", "fake_something", ["fake_file"])]

        with patch("builtins.open", mock_open(read_data="fake_data")) as mock_file:
            mounts = self.config_applier.apply_configs(self.host, self.node, self.config_paths, self.config_vars)

            self.assertEqual(mounts, {
                "/fake_binary_path/sub_fake_config_path/fake_file": "/usr/share/opensearch/sub_fake_config_path/fake_file"
            })
            self.path_manager.create_path.assert_has_calls([
                mock.call(self.host, "/fake_binary_path/sub_fake_config_path")
            ])
            self.template_renderer.render_template_file.assert_has_calls([
                mock.call("/fake_config_path/sub_fake_config_path", self.config_vars, "/fake_config_path/sub_fake_config_path/fake_file")
            ])
            self.executor.execute.assert_has_calls([
                mock.call(self.host,
                          "cp /fake_binary_path/sub_fake_config_path/fake_file /fake_binary_path/sub_fake_config_path/fake_file")
            ])
            mock_file.assert_called_with("/fake_binary_path/sub_fake_config_path/fake_file", mode='a', encoding='utf-8')
