from functools import reduce

from osbenchmark.exceptions import SystemSetupError


class RepositoryUrlProvider:
    def __init__(self, template_renderer, artifact_variables_provider):
        self.template_renderer = template_renderer
        self.artifact_variables_provider = artifact_variables_provider

    def render_url_for_key(self, host, config_variables, key, mandatory=True):
        try:
            url_template = self._get_value_from_dot_notation_key(config_variables, key)
        except TypeError:
            if mandatory:
                raise SystemSetupError(f"Config key [{key}] is not defined.")
            else:
                return None

        artifact_version = config_variables["distribution"]["version"]
        artifact_variables = self.artifact_variables_provider.get_artifact_variables(host, artifact_version)
        return self.template_renderer.render_template_string(url_template, artifact_variables)

    def _get_value_from_dot_notation_key(self, dict_object, key):
        return reduce(dict.get, key.split("."), dict_object)
