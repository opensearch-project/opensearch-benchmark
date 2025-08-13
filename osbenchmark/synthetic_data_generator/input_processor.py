# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging

from osbenchmark.synthetic_data_generator.models import SyntheticDataGeneratorMetadata
from osbenchmark.exceptions import ConfigError

logger = logging.getLogger(__name__)

def create_sdg_metadata_from_args(cfg) -> SyntheticDataGeneratorMetadata:
    """
    Creates a Synthetic Data Generator Config based on the user's inputs

    :param cfg: Contains the command line configuration

    :return: A dataclass that contains configuration and information user provided
    """
    try:
        index_mappings_path = cfg.opts("synthetic_data_generator", "index_mappings")
        custom_module_path = cfg.opts("synthetic_data_generator", "custom_module")
        custom_config_path = cfg.opts("synthetic_data_generator", "custom_config")

        return SyntheticDataGeneratorMetadata(
            index_name = cfg.opts("synthetic_data_generator", "index_name"),
            index_mappings_path = index_mappings_path,
            custom_module_path = custom_module_path,
            custom_config_path = custom_config_path,
            output_path = cfg.opts("synthetic_data_generator", "output_path"),
            total_size_gb= cfg.opts("synthetic_data_generator", "total_size"),
        )

    except ConfigError as e:
        raise ConfigError("Config error when building SyntheticDataGeneratorMetadata: ", e)

def use_custom_synthetic_data_generator(sdg_metadata: SyntheticDataGeneratorMetadata) -> bool:
    if sdg_metadata.custom_module_path and not sdg_metadata.index_mappings_path:
        logger.info("User is using custom module to generate synthetic data. Custom module is found in this path: [%s]", sdg_metadata.custom_module_path)
        return True

    return False

def use_mappings_synthetic_data_generator(sdg_metadata: SyntheticDataGeneratorMetadata) -> bool:
    if sdg_metadata.index_mappings_path and not sdg_metadata.custom_module_path:
        logger.info("User is using index mappings to generate synthetic data. Index mappings are found in this path: [%s]", sdg_metadata.index_mappings_path)
        return True

    return False
