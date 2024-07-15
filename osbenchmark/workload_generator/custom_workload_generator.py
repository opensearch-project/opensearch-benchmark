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
from osbenchmark.workload_generator.config import CustomWorkload, Index
from osbenchmark.workload_generator.helpers import QueryProcessor, CustomWorkloadWriter
from osbenchmark.workload_generator.extractors import IndexExtractor, SynchronousCorpusExtractor
from osbenchmark.utils import io, opts, console

def create_workload(cfg):
    logger = logging.getLogger(__name__)

    # All inputs provided by user
    workload_name: str = cfg.opts("workload", "workload.name")
    indices: list = cfg.opts("generator", "indices")
    root_path: str = cfg.opts("generator", "output.path")
    target_hosts: opts.TargetHosts = cfg.opts("client", "hosts")
    client_options: opts.ClientOptions = cfg.opts("client", "options")
    document_frequency: int = cfg.opts("generator", "document_frequency")
    limit_documents: dict = cfg.opts("generator", "limit_documents") # Replaces number_of_docs, a map with key of index name and value of documents count to extract
    unprocessed_queries: dict = cfg.opts("workload", "custom_queries")
    templates_path: str = os.path.join(cfg.opts("node", "benchmark.root"), "resources")

    # Validation
    validate_index_documents_map(indices, limit_documents)

    client = OsClientFactory(hosts=target_hosts.all_hosts[opts.TargetHosts.DEFAULT],
                             client_options=client_options.all_client_options[opts.TargetHosts.DEFAULT]).create()
    info = client.info()
    console.info(f"Connected to OpenSearch cluster [{info['name']}] version [{info['version']['number']}].\n", logger=logger)

    processed_indices = process_indices(indices, document_frequency, limit_documents)

    custom_workload = CustomWorkload(
        workload_name=workload_name,
        root_path=root_path,
        indices=processed_indices,
    )
    custom_workload.workload_path = os.path.abspath(os.path.join(io.normalize_path(root_path), workload_name))
    custom_workload.operations_path = os.path.join(custom_workload.workload_path, "operations")
    custom_workload.test_procedures_path = os.path.join(custom_workload.workload_path, "test_procedures")

    query_processor = QueryProcessor(unprocessed_queries)
    custom_workload_writer = CustomWorkloadWriter(root_path, workload_name, templates_path)
    index_extractor = IndexExtractor(custom_workload, client)
    corpus_extractor = SynchronousCorpusExtractor(custom_workload, client)

    # Process Queries
    processed_queries = query_processor.process_queries()
    custom_workload.queries = processed_queries
    logger.info("Processed custom queries [%s]", custom_workload.queries)

    # Create Workload Output Path
    custom_workload_writer.make_workload_directory()
    logger.info("Created workload output path at [%s]", custom_workload.workload_path)

    # Extract Index Settings and Mappings
    index_extractor.extract_indices(custom_workload.workload_path)
    logger.info("Extracted index settings and mappings from [%s]", custom_workload.indices)

    # Extract Corpora
    for index in custom_workload.indices:
        index_corpora = corpus_extractor.extract_documents(index.name, index.limit_documents)
        custom_workload.corpora.append(index_corpora)
    logger.info("Extracted all corpora [%s]", custom_workload.corpora)

    if len(custom_workload.corpora) == 0:
        raise exceptions.BenchmarkError("Failed to extract corpora for any indices for workload!")

    template_vars = {
        "workload_name": custom_workload.workload_name,
        "indices": indices, # use this instead of custom_workload.workload_name because we need the names only
        "corpora": custom_workload.corpora,
        "custom_queries": custom_workload.queries
    }

    logger.info("Template vars [%s]", template_vars)

    # Create Workload
    workload_file_path = os.path.join(custom_workload.workload_path, "workload.json")
    operations_file_path = os.path.join(custom_workload.operations_path, "default.json")
    test_procedures_file_path = os.path.join(custom_workload.test_procedures_path, "default.json")

    # Render all templates
    logger.info("Rendering templates")
    custom_workload_writer.render_templates(
        workload_file_path,
        operations_file_path,
        test_procedures_file_path,
        templates_path,
        template_vars,
        custom_workload.queries
    )

    console.println("")
    console.info(f"Workload {workload_name} has been created. Run it with: {PROGRAM_NAME} --workload-path={custom_workload.workload_path}")


def process_indices(indices, document_frequency, limit_documents):
    processed_indices = []
    for index_name in indices:
        index = Index(
            name=index_name,
            document_frequency=document_frequency,
            limit_documents=limit_documents
        )
        processed_indices.append(index)

    return processed_indices


def validate_index_documents_map(indices, indices_docs_map):
    if indices_docs_map is not None and len(indices_docs_map) > 0:
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

