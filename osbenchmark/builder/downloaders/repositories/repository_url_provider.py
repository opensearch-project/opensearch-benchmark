from functools import reduce

from osbenchmark.builder.utils.template_renderer import TemplateRenderer
from osbenchmark.exceptions import SystemSetupError

ARCH_MAPPINGS = {
    "x86_64": "x64",
    "aarch64": "arm64"
}


class RepositoryUrlProvider:
    def __init__(self, executor):
        self.executor = executor
        self.template_renderer = TemplateRenderer()

    def render_url_for_key(self, host, config_variables, key, mandatory=True):
        try:
            url_template = self._get_value_from_dot_notation_key(config_variables, key)
        except TypeError:
            if mandatory:
                raise SystemSetupError("Config key [{}] is not defined.".format(key))
            else:
                return None
        return self.template_renderer.render_template_string(url_template, self._get_url_template_variables(host, config_variables))

    def _get_value_from_dot_notation_key(self, dict_object, key):
        return reduce(dict.get, key.split("."), dict_object)

    def _get_url_template_variables(self, host, config_variables):
        return {
            "VERSION": config_variables["distribution"]["version"],
            "OSNAME": self._get_os_name(host),
            "ARCH": self._get_arch(host)
        }

    def _get_os_name(self, host):
        os_name = self.executor.execute(host, "uname", output=True)[0]
        return os_name.lower()

    def _get_arch(self, host):
        arch = self.executor.execute(host, "uname -m", output=True)[0]
        return ARCH_MAPPINGS[arch.lower()]
