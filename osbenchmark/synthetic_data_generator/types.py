# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import os
from dataclasses import dataclass, field
from typing import Optional

GB_TO_BYTES = 1024 ** 3

DEFAULT_GENERATION_SETTINGS = {
    "workers": os.cpu_count(),
    "max_file_size_gb": 40,
    "docs_per_chunk": 10000,
    "filename_suffix_begins_at": 0,
    "timeseries_enabled": {}
}

@dataclass
class SyntheticDataGeneratorMetadata:
    index_name: Optional[str] = None
    index_mappings_path: Optional[str] = None
    custom_module_path: Optional[str] = None
    custom_config_path: Optional[str] = None
    output_path: Optional[str] = None
    total_size_gb: Optional[int] = None
    mode: Optional[str] = None
    checkpoint: Optional[str] = None
    blueprint: dict = None
    generators: dict = field(default_factory=dict)
