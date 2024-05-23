import json
import os
import logging
import shutil

from opensearchpy import OpenSearchException
from jinja2 import Environment, FileSystemLoader, select_autoescape

from osbenchmark import PROGRAM_NAME, exceptions
from osbenchmark.utils import io, opts, console
from osbenchmark.workload_generator.config import CustomWorkload

class CustomWorkloadWriter:
    def __init__(self, custom_workload: CustomWorkload, templates_path: str):
        self.custom_workload: CustomWorkload = custom_workload
        self.templates_path: str = templates_path

        self.custom_workload.workload_path = os.path.abspath(os.path.join(io.normalize_path(self.root_path), self.workload_name))
        self.custom_workload.operations_path = os.path.join(self.workload_path, "operations")
        self.custom_workload.test_procedures_path = os.path.join(self.workload_path, "test_procedures")
        self.logger = logging.getLogger(__name__)

    def make_workload_directory(self):
        if os.path.exists(self.workload_path):
            try:
                self.logger.info("Workload already exists. Removing existing workload [%s] in path [%s]", self.workload_name, self.workload_path)
                shutil.rmtree(self.workload_path)
            except OSError:
                self.logger.error("Had issues removing existing workload [%s] in path [%s]", self.workload_name, self.workload_path)

        io.ensure_dir(self.custom_workload.workload_path)
        io.ensure_dir(self.custom_workload.operations_path)
        io.ensure_dir(self.custom_workload.test_procedures_path)

    def render_templates(self,
                        template_vars: dict,
                        custom_queries: dict):

        self._write_template(template_vars, "base-workload")

        if custom_queries:
            self._write_template(template_vars, "custom-operations")
            self._write_template(template_vars, "custom-test-procedures")
        else:
            self._write_template(template_vars, "default-operations")
            self._write_template(template_vars, "default-test-procedures")

    def _write_template(self, template_vars: dict, template_file: str):
        template = self._get_default_template(template_file)
        with open(self.workload_path, "w") as f:
            f.write(template.render(template_vars))

    def _get_default_template(self, template_file: str):
        template_file_name = template_file  + ".json.j2"

        env = Environment(loader=FileSystemLoader(self.templates_path), autoescape=select_autoescape(['html', 'xml']))

        return env.get_template(template_file_name)

class QueryProcessor:
    def __init__(self, queries: str):
        self.queries = queries

    def process_queries(self):
        if not self.queries:
            return []

        with self.queries as queries:
            try:
                processed_queries = json.load(queries)
                if isinstance(data, dict):
                    data = [data]
            except ValueError as err:
                raise exceptions.SystemSetupError(f"Ensure JSON schema is valid and queries are contained in a list: {err}")

        return processed_queries
