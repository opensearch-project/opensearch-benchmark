# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import bz2
import json
import logging
import os
from abc import ABC, abstractmethod

from osbenchmark.utils import console

DOCS_COMPRESSOR = bz2.BZ2Compressor
COMP_EXT = ".bz2"

class CorpusExtractor(ABC):

    @abstractmethod
    def extract_documents(self):
        pass


class SynchronousCorpusExtractor(CorpusExtractor):
    def __init__(self, client, output_path):
        self.client = client
        self.output_path = output_path

    def template_vars(self,index_name, out_path, doc_count):
        comp_outpath = out_path + COMP_EXT
        return {
            "index_name": index_name,
            "filename": os.path.basename(comp_outpath),
            "path": comp_outpath,
            "doc_count": doc_count,
            "uncompressed_bytes": os.path.getsize(out_path),
            "compressed_bytes": os.path.getsize(comp_outpath)
        }


    def _get_doc_outpath(self, outdir, name, suffix=""):
        return os.path.join(outdir, f"{name}-documents{suffix}.json")


    def extract(self, client, output_path, index, number_of_docs_requested=None):
        """
        Scroll an index with a match-all query, dumping document source to ``outdir/documents.json``.

        :param client: OpenSearch client used to extract data
        :param output_path: Destination directory for corpus dump
        :param index: Name of index to dump
        :return: dict of properties describing the corpus for templates
        """

        logger = logging.getLogger(__name__)

        number_of_docs = client.count(index=index)["count"]

        total_docs = number_of_docs if not number_of_docs_requested else min(number_of_docs, number_of_docs_requested)

        if total_docs > 0:
            logger.info("[%d] total docs in index [%s]. Extracting [%s] docs.", number_of_docs, index, total_docs)
            docs_path = self._get_doc_outpath(output_path, index)
            self.dump_documents(client, index, self._get_doc_outpath(output_path, index, "-1k"), min(total_docs, 1000), " for test mode")
            self.dump_documents(client, index, docs_path, total_docs)
            return self.template_vars(index, docs_path, total_docs)
        else:
            logger.info("Skipping corpus extraction fo index [%s] as it contains no documents.", index)
            return None


    def dump_documents(self, client, index, out_path, number_of_docs, progress_message_suffix=""):
        # pylint: disable=import-outside-toplevel
        from opensearchpy import helpers

        logger = logging.getLogger(__name__)
        freq = max(1, number_of_docs // 1000)

        progress = console.progress()
        compressor = DOCS_COMPRESSOR()
        comp_outpath = out_path + COMP_EXT
        with open(out_path, "wb") as outfile:
            with open(comp_outpath, "wb") as comp_outfile:
                logger.info("Dumping corpus for index [%s] to [%s].", index, out_path)
                query = {"query": {"match_all": {}}}
                for n, doc in enumerate(helpers.scan(client, query=query, index=index)):
                    if n >= number_of_docs:
                        break
                    data = (json.dumps(doc["_source"], separators=(",", ":")) + "\n").encode("utf-8")

                    outfile.write(data)
                    comp_outfile.write(compressor.compress(data))

                    self.render_progress(progress, progress_message_suffix, index, n + 1, number_of_docs, freq)

                comp_outfile.write(compressor.flush())
        progress.finish()


    def render_progress(self, progress, progress_message_suffix, index, cur, total, freq):
        if cur % freq == 0 or total - cur < freq:
            msg = f"Extracting documents for index [{index}]{progress_message_suffix}..."
            percent = (cur * 100) / total
            progress.print(msg, f"{cur}/{total} docs [{percent:.1f}% done]")
