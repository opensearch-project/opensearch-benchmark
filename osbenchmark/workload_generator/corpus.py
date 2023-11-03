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
from tqdm import tqdm

DOCS_COMPRESSOR = bz2.BZ2Compressor
COMP_EXT = ".bz2"
OUT_EXT = ".json"


def template_vars(index_name, out_path, doc_count):
    comp_outpath = out_path + OUT_EXT + COMP_EXT
    out_path = out_path + OUT_EXT
    return {
        "index_name": index_name,
        "filename": os.path.basename(comp_outpath),
        "path": comp_outpath,
        "doc_count": doc_count,
        "uncompressed_bytes": os.path.getsize(out_path),
        "compressed_bytes": os.path.getsize(comp_outpath),
    }


def get_doc_outpath(outdir, name, suffix=""):
    return os.path.join(outdir, f"{name}-documents{suffix}")


def extract(
    client, output_path, index, number_of_docs_requested=None, concurrent=False
):
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
            concurrent,
            client,
            index,
            get_doc_outpath(output_path, index, "-1k"),
            min(total_docs, 1000),
            "for test mode",
        )
        dump_documents(concurrent, client, index, docs_path, total_docs)
        return template_vars(index, docs_path, total_docs)
    else:
        logger.info(
            "Skipping corpus extraction fo index [%s] as it contains no documents.",
            index,
        )
        return None


def dump_documents_range(
    pbar,
    client,
    index,
    out_path,
    start_doc,
    end_doc,
    total_docs,
    progress_message_suffix="",
):
    """
    Extract documents in the range of start_doc and end_doc and write to individual files

    :param client: OpenSearch client used to extract data
    :param index: Name of OpenSearch index to extract documents from
    :param out_path: Destination directory for corpus dump
    :param start_doc: Start index of the document chunk
    :param end_doc: End index of the document chunk
    :param total_docs: Total number of documents
    :return: dict of properties describing the corpus for templates
    """

    logger = logging.getLogger(__name__)

    compressor = DOCS_COMPRESSOR()
    out_path = f"{out_path}_{start_doc}_{end_doc}" + OUT_EXT
    comp_outpath = out_path + COMP_EXT

    with open(out_path, "wb") as outfile:
        with open(comp_outpath, "wb") as comp_outfile:
            logger.info(
                "Dumping corpus for index [%s] to [%s] for docs %s-%s.",
                index,
                out_path,
                start_doc,
                end_doc,
            )
            query = {
                "query": {"match_all": {}},
                "from": start_doc,
                "size": end_doc - start_doc,
            }

            batch_size = (end_doc - start_doc) // 5
            search_after = None
            n = 0

            while n < (end_doc - start_doc):
                if search_after:
                    query = {
                        "query": {"match_all": {}},
                        "size": batch_size,
                        "sort": [{"_id": "asc"}],
                        "search_after": search_after,
                    }
                else:
                    query = {
                        "query": {"match_all": {}},
                        "size": batch_size,
                        "sort": [{"_id": "asc"}],
                        "from": start_doc,
                    }

                response = client.search(index=index, body=query)
                hits = response["hits"]["hits"]

                if not hits:
                    break

                for doc in hits:
                    try:
                        search_after = doc["sort"]
                    except KeyError:
                        print(doc)
                        logger.info("%s", doc)
                    data = (
                        json.dumps(doc["_source"], separators=(",", ":")) + "\n"
                    ).encode("utf-8")

                    outfile.write(data)
                    comp_outfile.write(compressor.compress(data))

                    n += 1
                    pbar.update(1)
                    if n >= (end_doc - start_doc):
                        break

            comp_outfile.write(compressor.flush())


def dump_documents(
    concurrent, client, index, out_path, number_of_docs, progress_message_suffix=""
):
    """
    Splits the dumping process into 8 threads.
    First, they split the documents into chunks to be dumped. Then, they are dumped as "{index}-documents{suffix}_{start}_{end}.json(.bz2)"
    Finally, they are all collated into their file "{out_path}-documents{suffix}.json(.bz2)" format.

    :param client: OpenSearch client used to extract data
    :param index: Name of OpenSearch index to extract documents from
    :param out_path: Destination directory for corpus dump
    :param number_of_docs: Total number of documents
    """
    if concurrent:
        num_threads = 8
        with tqdm(
            total=number_of_docs,
            desc="Extracting documents"
            + (f" [{progress_message_suffix}]" if progress_message_suffix else ""),
            unit="doc",
        ) as pbar:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                step = number_of_docs // num_threads
                ranges = [(i, i + step) for i in range(0, number_of_docs, step)]
                executor.map(
                    lambda args: dump_documents_range(
                        pbar,
                        client,
                        index,
                        out_path,
                        *args,
                        number_of_docs,
                        progress_message_suffix,
                    ),
                    ranges,
                )
            merge_json_files(out_path, ranges)
    else:
        with tqdm(
            total=number_of_docs,
            desc="Extracting documents"
            + (f" [{progress_message_suffix}]" if progress_message_suffix else ""),
            unit="doc",
        ) as pbar:
            dump_documents_range(
                pbar,
                client,
                index,
                out_path,
                0,
                number_of_docs,
                number_of_docs,
                progress_message_suffix,
            )


def merge_json_files(out_path, ranges):
    for EXT in [OUT_EXT, OUT_EXT + COMP_EXT]:
        merged_file_path = f"{out_path}" + EXT
        with open(merged_file_path, "wb") as merged_file:
            for start, end in ranges:
                file_path = f"{out_path}_{start}_{end}" + EXT
                with open(file_path, "rb") as f:
                    for line in f:
                        merged_file.write(line)
                os.remove(file_path)
