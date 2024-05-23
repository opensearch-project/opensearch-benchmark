# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import json
import logging
import os

from opensearchpy import OpenSearchException

class IndexExtractor:
    def __init__(self, indices, client):
        self.indices = indices
        self.client = client

        self.INDEX_SETTINGS_EPHEMERAL_KEYS = ["uuid",
                                        "creation_date",
                                        "version",
                                        "provided_name",
                                        "store"]
        self.INDEX_SETTINGS_PARAMETERS = {
            "number_of_replicas": "{{{{number_of_replicas | default({orig})}}}}",
            "number_of_shards": "{{{{number_of_shards | default({orig})}}}}"
        }
        self.logger = logging.getLogger(__name__)

    def extract_indices(self, workload_path):
        try:
            for index in self.indices:
                self.extract(workload_path, index)
        except OpenSearchException:
            self.logger("Failed at extracting index [%s]", index)

    def extract(self, outdir, index_pattern):
        """
        Request index information to format in "index.json" for Benchmark
        :param outdir: destination directory
        :param index_pattern: name of index
        :return: Dict of template variables representing the index for use in workload
        """
        results = []

        index_obj = self.extract_index_mapping_and_settings(index_pattern)
        for index, details in index_obj.items():
            filename = f"{index}.json"
            outpath = os.path.join(outdir, filename)
            with open(outpath, "w") as outfile:
                json.dump(details, outfile, indent=4, sort_keys=True)
                outfile.write("\n")
            results.append({
                "name": index,
                "path": outpath,
                "filename": filename,
            })
        return results

    def extract_index_mapping_and_settings(self, index_pattern):
        """
        Calls index GET to retrieve mapping + settings, filtering settings
        so they can be used to re-create this index
        :param index_pattern: name of index
        :return: index creation dictionary
        """
        results = {}
        logger = logging.getLogger(__name__)
        # the response might contain multiple indices if a wildcard was provided
        response = self.client.indices.get(index_pattern)
        for index, details in response.items():
            valid, reason = self.is_valid(index)
            if valid:
                mappings = details["mappings"]
                index_settings = self.filter_ephemeral_index_settings(details["settings"]["index"])
                self.update_index_setting_parameters(index_settings)
                results[index] = {
                    "mappings": mappings,
                    "settings": {
                        "index": index_settings
                    }
                }
            else:
                logger.info("Skipping index [%s] (reason: %s).", index, reason)

        return results

    def filter_ephemeral_index_settings(self, settings):
        """
        Some of the 'settings' published by OpenSearch for an index are
        ephemeral values, not useful for re-creating the index.
        :param settings: Index settings published by index.get()
        :return: settings with ephemeral keys removed
        """
        filtered = dict(settings)
        for s in self.INDEX_SETTINGS_EPHEMERAL_KEYS:
            filtered.pop(s, None)
        return filtered


    def update_index_setting_parameters(self, settings):
        for s, param in self.INDEX_SETTINGS_PARAMETERS.items():
            if s in settings:
                orig_value = settings[s]
                settings[s] = param.format(orig=orig_value)


    def is_valid(self, index_name):
        if len(index_name) == 0:
            return False, "Index name is empty"
        if index_name.startswith("."):
            return False, f"Index [{index_name}] is hidden"
        return True, None
