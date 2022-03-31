import jinja2
from jinja2 import select_autoescape

from osbenchmark.exceptions import InvalidSyntax, SystemSetupError
from osbenchmark.utils import io


class TemplateRenderer:
    def render_template(self, root_path, variables, file_name):
        try:
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(root_path), autoescape=select_autoescape(['html', 'xml']))
            template = env.get_template(io.basename(file_name))
            # force a new line at the end. Jinja seems to remove it.
            return template.render(variables) + "\n"
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise InvalidSyntax("%s in %s" % (str(e), file_name))
        except BaseException as e:
            raise SystemSetupError("%s in %s" % (str(e), file_name))
