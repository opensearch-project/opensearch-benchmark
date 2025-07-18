# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import os
import re
import logging
import json
import shutil
import importlib.util
import pickle

import yaml

from osbenchmark.utils import console
from osbenchmark.exceptions import SystemSetupError, ExecutorError
from osbenchmark.synthetic_data_generator.types import DEFAULT_GENERATION_SETTINGS, SyntheticDataGeneratorMetadata, GB_TO_BYTES

def load_user_module(file_path):
    allowed_extensions = ['.py']
    extension = os.path.splitext(file_path)[1]
    if extension not in allowed_extensions:
        raise SystemSetupError(f"User provided module with file extension [{extension}]. Python modules must have {allowed_extensions} extension.")

    spec = importlib.util.spec_from_file_location("user_module", file_path)
    user_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_module)
    return user_module

def load_mapping(mapping_file_path):
    """
    Loads an index mapping from a JSON file.

    Args:
        mapping_file_path (str): Path to the index mappings JSON file

    Returns:
        mapping as dict
    """
    with open(mapping_file_path, "r") as f:
        mapping_dict = json.load(f)

    return mapping_dict

def check_for_exsiting_files(output_path: str, index_name: str):
    logger = logging.getLogger(__name__)
    existing_files_found = existing_files_found_in_output_dir(output_path, index_name)
    if existing_files_found:
        user_decision = input(f"Files with the same expected names were found in the output directory {output_path}. " + \
                                "Would you like to remove them (so that SDG does not append to them)? (y/n): ")
        while user_decision.lower() not in ['y', 'n']:
            user_decision = input(f"Files with the same expected names were found in the output directory {output_path}. " + \
                                "Would you like to remove them (so that SDG does not append to them)? (y/n): ")

        if user_decision.lower() == "y":
            remove_existing_files(existing_files_found)
            logger.info("Files have been removed at: %s ", output_path)
            console.println(f"Files have been removed at: {output_path}\n")
        else:
            logger.info("Keeping files at: %s", output_path)
            console.println(f"Keeping files at: {output_path}\n")

def existing_files_found_in_output_dir(output_path: str, index_name: str) -> bool:
    existing_files = []

    for file in os.listdir(output_path):
        if (file.startswith(index_name) and file.endswith(".json")) or (file.startswith(index_name) and file.endswith('_record.json')):
            existing_files.append(os.path.join(output_path, file))

    return existing_files

def remove_existing_files(existing_files_found: list[str]):
    try:
        for file in existing_files_found:
            os.remove(file)
    except FileNotFoundError as e:
        raise ExecutorError("OSB could not remove existing files for SDG because it does not exist: ", e)
    except OSError as e:
        raise ExecutorError("OSB could not remove existing files for SDG: ", e)

def host_has_available_disk_storage(sdg_metadata: SyntheticDataGeneratorMetadata) -> bool:
    logger = logging.getLogger(__name__)
    try:
        requested_size_in_bytes = sdg_metadata.total_size_gb * GB_TO_BYTES
        output_path_directory = sdg_metadata.output_path if os.path.isdir(sdg_metadata.output_path) else os.path.dirname(sdg_metadata.output_path)

        free_storage = shutil.disk_usage(output_path_directory)[2]
        logger.info("Host has [%s] bytes of available storage.", free_storage)

        return free_storage >= requested_size_in_bytes
    except Exception:
        logger.error("Error checking disk space.")
        return False

def load_config(config_path):
    try:
        allowed_extensions = ['.yml', '.yaml']

        extension = os.path.splitext(config_path)[1]
        if extension not in allowed_extensions:
            raise SystemSetupError(f"User provided config with extension [{extension}]. Config must have a {allowed_extensions} extension.")
        else:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
    except TypeError:
        raise SystemSetupError("Error when loading config. Please ensure that the proper config was provided")

def write_chunk(data, file_path):
    written_bytes = 0
    with open(file_path, 'a') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
            written_bytes += len(pickle.dumps(item))
    return len(data), written_bytes

def get_generation_settings(input_config: dict) -> dict:
    '''
    Grabs the user's config's generation settings and compares it with the default generation settings.
    If there are missing fields in the user's config, it populates it with the default values
    '''
    generation_settings: dict[str, int | None] = DEFAULT_GENERATION_SETTINGS
    if input_config is None: # if user did not provide a custom config
        return generation_settings

    user_generation_settings = input_config.get('settings', {})

    if not user_generation_settings: # If user provided custom config but did not include settings
        return generation_settings
    else:
        # Traverse and update valid settings that user specified.
        for k in generation_settings:
            if k in user_generation_settings and user_generation_settings[k] is not None:
                generation_settings[k] = user_generation_settings[k]
            else:
                continue

        return generation_settings

def format_size(bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024
    return f"{bytes:.2f} PB"

def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

def setup_custom_tqdm_formatting(progress_bar):
    progress_bar.format_dict['n_fmt'] = lambda n: format_size(n) # pylint: disable=unnecessary-lambda
    progress_bar.format_dict['total_fmt'] = lambda t: format_size(t) # pylint: disable=unnecessary-lambda
    progress_bar.format_dict['elapsed'] = lambda e: format_time(e) # pylint: disable=unnecessary-lambda
    progress_bar.format_dict['remaining'] = lambda r: format_time(r) # pylint: disable=unnecessary-lambda

def build_record(sdg_metadata: SyntheticDataGeneratorMetadata, total_time_to_generate_dataset, generated_dataset_details: dict) -> dict:
    total_docs_written = 0
    total_dataset_size_in_bytes = 0
    for file in generated_dataset_details:
        total_docs_written += file["docs"]
        total_dataset_size_in_bytes += file["file_size_bytes"]

    record = {
        "index-name": sdg_metadata.index_name,
        "output_path": sdg_metadata.output_path,
        "total-docs-written": total_docs_written,
        "total-dataset-size": total_dataset_size_in_bytes,
        "total-time-to-generate-dataset": total_time_to_generate_dataset,
        "files": generated_dataset_details
    }

    return record

def write_record(sdg_metadata: SyntheticDataGeneratorMetadata, record):
    path = os.path.join(sdg_metadata.output_path, f"{sdg_metadata.index_name}_record.json")
    with open(path, 'w') as file:
        json.dump(record, file, indent=2)

def write_record_and_publish_summary_to_console(sdg_metadata: SyntheticDataGeneratorMetadata, total_time_to_generate_dataset: int, generated_dataset_details: dict):
    logger = logging.getLogger(__name__)

    record = build_record(sdg_metadata, total_time_to_generate_dataset, generated_dataset_details)
    write_record(sdg_metadata, record)

    summary = f"Generated {record['total-docs-written']} docs in {total_time_to_generate_dataset} seconds. Total dataset size is {record['total-dataset-size'] / (1000 ** 3)}GB."
    console.println("")
    console.println(summary)

    logger.info("Visit the following path to view synthetically generated data: [%s]", sdg_metadata.output_path)
    console.println(f"✅ Visit the following path to view synthetically generated data: {sdg_metadata.output_path}")
