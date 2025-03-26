# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import argparse
import os
from dataclasses import dataclass, field
from typing import List, Optional

from osbenchmark.utils import io, opts, console
from enum import Enum

DEFAULT_MAX_FILE_SIZE_GB=40
DEFAULT_CHUNK_SIZE=10000

DEFAULT_GENERATION_SETTINGS = {
    "workers": os.cpu_count(),
    "max_file_size_gb": 40,
    "chunk_size": 10000
}

@dataclass
class SyntheticDataGeneratorConfig:
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