# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import os
import json

from opensearchpy import OpenSearchException
from jinja2 import Environment, FileSystemLoader, select_autoescape

from osbenchmark import PROGRAM_NAME, exceptions
from osbenchmark.client import OsClientFactory
from osbenchmark.workload_generator import corpus, index
from osbenchmark.utils import io, opts, console


def process_template(templates_path, template_filename, template_vars, output_path):
    env = Environment(loader=FileSystemLoader(templates_path), autoescape=select_autoescape(['html', 'xml']))
    template = env.get_template(template_filename)

    with open(output_path, "w") as f:
        f.write(template.render(template_vars))

def validate_indices_docs_map(indices, indices_docs_map, docs_were_requested):
    if not docs_were_requested:
        return

    if len(indices) < len(indices_docs_map):
        raise exceptions.SystemSetupError(
            "Number of <index>:<doc_count> pairs exceeds number of indices in --indices. " +
            "Ensure number of <index>:<doc_count> pairs is less than or equal to number of indices in --indices."
        )

    for index_name in indices_docs_map:
        if index_name not in indices:
            raise exceptions.SystemSetupError(
                "Index from <index>:<doc_count> pair was not found in --indices. " +
                "Ensure that indices from all <index>:<doc_count> pairs exist in --indices."
            )

def extract_mappings_and_corpora(client, output_path, indices_to_extract, indices_docs_map):
    indices = []
    corpora = []
    docs_were_requested = indices_docs_map is not None and len(indices_docs_map) > 0

    validate_indices_docs_map(indices_to_extract, indices_docs_map, docs_were_requested)

    # first extract index metadata (which is cheap) and defer extracting data to reduce the potential for
    # errors due to invalid index names late in the process.
    for index_name in indices_to_extract:
        try:
            indices += index.extract(client, output_path, index_name)
        except OpenSearchException:
            logging.getLogger(__name__).exception("Failed to extract index [%s]", index_name)

    # That list only contains valid indices (with index patterns already resolved)
    # For each index, check if docs were requested. If so, extract the number of docs from the map
    for i in indices:
        custom_docs_to_extract = None

        if docs_were_requested and i["name"] in indices_docs_map:
            try:
                custom_docs_to_extract = int(indices_docs_map.get(i["name"]))
            except ValueError:
                raise exceptions.InvalidSyntax(
                    f"The string [{indices_docs_map.get(i['name'])}] in <index>:<doc_count> pair cannot be converted to an integer."
                )

        logging.getLogger(__name__).info("Extracting [%s] docs for index [%s]", custom_docs_to_extract, i["name"])
        c = corpus.extract(client, output_path, i["name"], custom_docs_to_extract)
        if c:
            corpora.append(c)

    return indices, corpora

def process_custom_queries(custom_queries):
    if not custom_queries:
        return []

    with custom_queries as queries:
        try:
            data = json.load(queries)
            if isinstance(data, dict):
                data = [data]
        except ValueError as err:
            raise exceptions.SystemSetupError(f"Ensure JSON schema is valid and queries are contained in a list: {err}")

    return data

def create_workload(cfg):
    logger = logging.getLogger(__name__)

    workload_name = cfg.opts("workload", "workload.name")
    indices = cfg.opts("generator", "indices")
    root_path = cfg.opts("generator", "output.path")
    target_hosts = cfg.opts("client", "hosts")
    client_options = cfg.opts("client", "options")
    number_of_docs = cfg.opts("generator", "number_of_docs")
    unprocessed_custom_queries = cfg.opts("workload", "custom_queries")

    custom_queries = process_custom_queries(unprocessed_custom_queries)

    logger.info("Creating workload [%s] matching indices [%s]", workload_name, indices)
    logger.info("Number of Docs: %s", number_of_docs)
    client = OsClientFactory(hosts=target_hosts.all_hosts[opts.TargetHosts.DEFAULT],
                             client_options=client_options.all_client_options[opts.TargetHosts.DEFAULT]).create()

    info = client.info()
    console.info(f"Connected to OpenSearch cluster [{info['name']}] version [{info['version']['number']}].\n", logger=logger)

    output_path = os.path.abspath(os.path.join(io.normalize_path(root_path), workload_name))
    io.ensure_dir(output_path)

    indices, corpora = extract_mappings_and_corpora(client, output_path, indices, number_of_docs)

    if len(indices) == 0:
        raise RuntimeError("Failed to extract any indices for workload!")

    template_vars = {
        "workload_name": workload_name,
        "indices": indices,
        "corpora": corpora,
        "custom_queries": custom_queries
    }

    logger.info("Template Vars: %s", template_vars)

    workload_path = os.path.join(output_path, "workload.json")
    templates_path = os.path.join(cfg.opts("node", "benchmark.root"), "resources")

    if custom_queries:
        process_template(templates_path, "custom-query-workload.json.j2", template_vars, workload_path)
    else:
        process_template(templates_path, "default-query-workload.json.j2", template_vars, workload_path)

    console.println("")
    console.info(f"Workload {workload_name} has been created. Run it with: {PROGRAM_NAME} --workload-path={output_path}")
