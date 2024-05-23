# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.


import logging
import os
import shutil
import json

from opensearchpy import OpenSearchException
from jinja2 import Environment, FileSystemLoader, select_autoescape

from osbenchmark import PROGRAM_NAME, exceptions
from osbenchmark.client import OsClientFactory
from osbenchmark.workload_generator.config import CustomWorkload, Index, Corpus
from osbenchmark.workload_generator.helpers import QueryProcessor, CustomWorkloadWriter
from osbenchmark.workload_generator.index_extractor import IndexExtractor
from osbenchmark.utils import io, opts, console

def create_workload(cfg):
    logger = logging.getLogger(__name__)

    # All inputs provided by user
    workload_name = cfg.opts("workload", "workload.name")
    indices = cfg.opts("generator", "indices")
    root_path = cfg.opts("generator", "output.path")
    target_hosts = cfg.opts("client", "hosts")
    client_options = cfg.opts("client", "options")
    document_frequency = cfg.opts("generator", "document_frequency")
    limit_documents = cfg.opts("generator", "limit_documents")
    unprocessed_queries = cfg.opts("workload", "custom_queries")
    templates_path = os.path.join(cfg.opts("node", "benchmark.root"), "resources")

    client = OsClientFactory(hosts=target_hosts.all_hosts[opts.TargetHosts.DEFAULT],
                             client_options=client_options.all_client_options[opts.TargetHosts.DEFAULT]).create()
    info = client.info()
    console.info(f"Connected to OpenSearch cluster [{info['name']}] version [{info['version']['number']}].\n", logger=logger)

    custom_workload = CustomWorkload(workload_name=workload_name, root_path=root_path)
    query_processor = QueryProcessor(unprocessed_queries)
    custom_workload_writer = CustomWorkloadWriter(root_path, workload_name, templates_path)
    index_extractor = IndexExtractor(indices, client)

    # Process Queries
    processed_queries = query_processor.process_queries()
    custom_workload.queries = processed_queries

    # Create Output Path
    custom_workload_writer.make_workload_directory()

    # Extract Index Settings and Mappings
    index_extractor.extract_indices(custom_workload.workload_path)

    # Extract Corpora

    # Product Workload
