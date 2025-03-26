import os
import logging
import yaml
import json

from osbenchmark.utils import console
from osbenchmark.exceptions import SystemSetupError
from osbenchmark.synthetic_data_generator.types import DEFAULT_GENERATION_SETTINGS, SyntheticDataGeneratorConfig

def load_config(config_path):
    try:
        allowed_extensions = ['.yml', '.yaml']

        extension = os.path.splitext(config_path)[1]
        if extension not in allowed_extensions:
            raise SystemSetupError(f"User provided config with extension [{extension}]. Config must have a {allowed_extensions} extension.")

        if config_path.endswith(allowed_extensions[0]) or config_path.endswith(allowed_extensions[1]):
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
    except TypeError as e:
        raise SystemSetupError("Error when loading config. Please ensure that the proper config was provided")

def write_chunk(data, file_path):
    with open(file_path, 'a') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return len(data)

def get_generation_settings(input_config: dict) -> dict:
    '''
    Grabs the user's config's generation settings and compares it with the default generation settings.
    If there are missing fields in the user's config, it populates it with the default values
    '''
    generation_settings = DEFAULT_GENERATION_SETTINGS
    if input_config is None:
        return generation_settings

    user_generation_settings = input_config.get('settings', {})

    if user_generation_settings == {}:
        return generation_settings
    else:
        # Traverse and update valid settings that user specified.
        for k, v in generation_settings.items():
            if k in user_generation_settings and user_generation_settings[k] is not None:
                generation_settings[k] = user_generation_settings[k]
            else:
                continue

        return generation_settings

def build_record(sdg_config: SyntheticDataGeneratorConfig, total_time_to_generate_dataset, generated_dataset_details: dict) -> dict:
    total_docs_written = 0
    total_dataset_size_in_bytes = 0
    for file in generated_dataset_details:
        total_docs_written += file["docs"]
        total_dataset_size_in_bytes += file["file_size_bytes"]

    record = {
        "index-name": sdg_config.index_name,
        "output_path": sdg_config.output_path,
        "total-docs-written": total_docs_written,
        "total-dataset-size": total_dataset_size_in_bytes,
        "total-time-to-generate-dataset": total_time_to_generate_dataset,
        "files": generated_dataset_details
    }

    return record

def write_record(sdg_config: SyntheticDataGeneratorConfig, record):
    path = os.path.join(sdg_config.output_path, f"{sdg_config.index_name}_record.json")
    with open(path, 'w') as file:
        json.dump(record, file, indent=2)

def write_record_and_publish_summary_to_console(sdg_config: SyntheticDataGeneratorConfig, total_time_to_generate_dataset: int, generated_dataset_details: dict):
    logger = logging.getLogger(__name__)

    record = build_record(sdg_config, total_time_to_generate_dataset, generated_dataset_details)
    write_record(sdg_config, record)

    summary = f"Generated {record['total-docs-written']} docs in {total_time_to_generate_dataset} seconds. Total dataset size is {record['total-dataset-size'] / (1000 ** 3)}GB."
    console.println("")
    console.println(summary)

    logger.info("Visit the following path to view synthetically generated data: [%s]", sdg_config.output_path)
    console.println(f"Visit the following path to view synthetically generated data: {sdg_config.output_path}")
