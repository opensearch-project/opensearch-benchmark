import jinja2
from jinja2 import select_autoescape

from osbenchmark.exceptions import InvalidSyntax, SystemSetupError
from osbenchmark.utils import io


class TemplateRenderer:
    def render_template_file(self, root_path, variables, file_name):
        return self._handle_template_rendering_exceptions(self._render_template_file, root_path, variables, file_name)

    def _render_template_file(self, root_path, variables, file_name):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(root_path), autoescape=select_autoescape(['html', 'xml']))
        template = env.get_template(io.basename(file_name))
        # force a new line at the end. Jinja seems to remove it.
        return template.render(variables) + "\n"

    def render_template_string(self, template_string, variables):
        return self._handle_template_rendering_exceptions(self._render_template_string, template_string, variables)

    def _render_template_string(self, template_string, variables):
        env = jinja2.Environment(loader=jinja2.BaseLoader, autoescape=select_autoescape(['html', 'xml']))
        template = env.from_string(template_string)

        return template.render(variables)

    def _handle_template_rendering_exceptions(self, render_func, *args):
        try:
            return render_func(*args)
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise InvalidSyntax("%s" % str(e))
        except BaseException as e:
            raise SystemSetupError("%s" % str(e))
