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
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import bz2
import json
import logging
import os

from concurrent.futures import ThreadPoolExecutor
from osbenchmark.utils import console


DOCS_COMPRESSOR = bz2.BZ2Compressor
COMP_EXT = ".bz2"


def template_vars(index_name, out_path, doc_count):
    comp_outpath = out_path + COMP_EXT
    return {
        "index_name": index_name,
        "filename": os.path.basename(comp_outpath),
        "path": comp_outpath,
        "doc_count": doc_count,
        "uncompressed_bytes": os.path.getsize(out_path),
        "compressed_bytes": os.path.getsize(comp_outpath),
    }


def get_doc_outpath(outdir, name, suffix=""):
    return os.path.join(outdir, f"{name}-documents{suffix}.json")


def extract(client, output_path, index, number_of_docs_requested=None):
    """
    Scroll an index with a match-all query, dumping document source to ``outdir/documents.json``.

    :param client: OpenSearch client used to extract data
    :param output_path: Destination directory for corpus dump
    :param index: Name of index to dump
    :return: dict of properties describing the corpus for templates
    """

    logger = logging.getLogger(__name__)

    number_of_docs = client.count(index=index)["count"]

    total_docs = (
        number_of_docs
        if not number_of_docs_requested
        else min(number_of_docs, number_of_docs_requested)
    )

    if total_docs > 0:
        logger.info(
            "[%d] total docs in index [%s]. Extracting [%s] docs.",
            number_of_docs,
            index,
            total_docs,
        )
        docs_path = get_doc_outpath(output_path, index)
        dump_documents(
            client,
            index,
            get_doc_outpath(output_path, index, "-1k"),
            min(total_docs, 1000),
            " for test mode",
        )
        dump_documents(client, index, docs_path, total_docs)
        return template_vars(index, docs_path, total_docs)
    else:
        logger.info(
            "Skipping corpus extraction fo index [%s] as it contains no documents.",
            index,
        )
        return None


def dump_documents_range(
    client, index, out_path, start_doc, end_doc, total_docs, progress_message_suffix=""
):
    # pylint: disable=import-outside-toplevel
    from opensearchpy import helpers

    logger = logging.getLogger(__name__)
    freq = max(1, total_docs // 1000)

    progress = console.progress()
    compressor = DOCS_COMPRESSOR()
    comp_outpath = out_path + COMP_EXT

    with open(f"{out_path}_{start_doc}_{end_doc}", "wb") as outfile:
        with open(f"{comp_outpath}_{start_doc}_{end_doc}", "wb") as comp_outfile:
            logger.info(
                f"Dumping corpus for index [{index}] to [{out_path}] for docs {start_doc}-{end_doc}."
            )
            query = {
                "query": {"match_all": {}},
                "from": start_doc,
                "size": end_doc - start_doc,
            }

            search_after = None

            n = 0

            while n < (end_doc - start_doc):
                if search_after:
                    query = {
                        "query": {"match_all": {}},
                        "size": 1,
                        "sort": [{"_id": "asc"}],
                        "search_after": search_after,
                    }
                else:
                    query = {
                        "query": {"match_all": {}},
                        "size": 1,
                        "sort": [{"_id": "asc"}],
                        "from": start_doc,
                    }

                response = client.search(index=index, body=query)
                hits = response["hits"]["hits"]

                if not hits:
                    break

                doc = hits[0]
                search_after = doc["sort"]
                data = (
                    json.dumps(doc["_source"], separators=(",", ":")) + "\n"
                ).encode("utf-8")

                outfile.write(data)
                comp_outfile.write(compressor.compress(data))

                render_progress(
                    progress,
                    progress_message_suffix,
                    index,
                    start_doc + n + 1,
                    total_docs,
                    freq,
                )

                n += 1

            comp_outfile.write(compressor.flush())


def dump_documents(client, index, out_path, number_of_docs, progress_message_suffix=""):
    num_threads = 8
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        step = number_of_docs // num_threads
        ranges = [(i, i + step) for i in range(0, number_of_docs, step)]
        executor.map(
            lambda args: dump_documents_range(
                client, index, out_path, *args, number_of_docs, progress_message_suffix
            ),
            ranges,
        )


def render_progress(progress, progress_message_suffix, index, cur, total, freq):
    if cur % freq == 0 or total - cur < freq:
        msg = f"Extracting documents for index [{index}]{progress_message_suffix}..."
        percent = (cur * 100) / total
        progress.print(msg, f"{cur}/{total} docs [{percent:.1f}% done]")
