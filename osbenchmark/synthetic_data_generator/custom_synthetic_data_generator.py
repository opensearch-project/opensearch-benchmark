import logging

import json
import time
import os
import numpy as np
import hashlib
import importlib.util
import yaml

import dask
from dask.distributed import Client, as_completed, get_client
from multiprocessing import Process, Queue
from mimesis import Generic
from mimesis.schema import Schema
from mimesis.locales import Locale
from mimesis.random import Random
from mimesis import Cryptographic
from mimesis.providers.base import BaseProvider
from mimesis.random import Random
from tqdm import tqdm

from osbenchmark.utils import console
from osbenchmark.exceptions import SystemSetupError, ConfigError
# from osbenchmark.synthetic_data_generator.input_processor import create_sdg_config_from_args, use_custom_synthetic_data_generator
from osbenchmark.synthetic_data_generator.types import DEFAULT_MAX_FILE_SIZE_GB, DEFAULT_CHUNK_SIZE
from osbenchmark.synthetic_data_generator.helpers import write_chunk, get_generation_settings

def load_user_module(file_path):
    allowed_extensions = ['.py']
    extension = os.path.splitext(file_path)[1]
    if extension not in allowed_extensions:
        raise SystemSetupError(f"User provided module with file extension [{extension}]. Python modules must have {allowed_extensions} extension.")

    spec = importlib.util.spec_from_file_location("user_module", file_path)
    user_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_module)
    return user_module

def generate_seeds_for_workers(regenerate=False):
    # This adds latency so might consider deprecating this
    client = get_client()
    workers = client.scheduler_info()['workers']

    seeds = []
    for worker_id in workers.keys():
        hash_object = hashlib.md5(worker_id.encode())

        if regenerate:
            # Add current timestamp to each hash to improve uniqueness
            timestamp = str(time.time()).encode()
            hash_object.update(timestamp)

        hash_hex = hash_object.hexdigest()

        seed = int(hash_hex[:8], 16)
        seeds.append(seed)

    return seeds

def get_avg_document_size(generate_fake_document: callable, custom_providers: dict, custom_lists: dict) -> int:
    output = [generate_test_document(generate_fake_document, custom_lists, custom_providers)]
    write_chunk(output, '/tmp/test-size.json')

    size = os.path.getsize('/tmp/test-size.json')
    os.remove('/tmp/test-size.json')

    return size

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
    """Set up custom formatting for the tqdm progress bar."""
    progress_bar.format_dict['n_fmt'] = lambda n: format_size(n)
    progress_bar.format_dict['total_fmt'] = lambda t: format_size(t)
    progress_bar.format_dict['elapsed'] = lambda e: format_time(e)
    progress_bar.format_dict['remaining'] = lambda r: format_time(r)

def instantiate_all_providers(custom_providers):
    g = Generic(locale=Locale.DEFAULT)
    r = Random()

    if custom_providers:
        g = add_custom_providers(g, custom_providers)

    provider_instances = {
        'generic': g,
        'random': r
    }

    return provider_instances

def seed_providers(providers, seed=None):
    for key, provider_instance in providers.items():
        if key in ['generic']:
            provider_instance.reseed(seed)
        elif key in ['random']:
            provider_instance.seed(seed)

    return providers

def add_custom_providers(generic, custom_providers):
    for name, provider_class in custom_providers.items():
        if issubclass(provider_class, BaseProvider):
            generic.add_provider(provider_class)
        else:
            # If it's not a Mimesis provider, we'll add it as is
            setattr(generic, name, provider_class())
    return generic

def generate_test_document(generate_fake_document: callable, custom_lists: dict, custom_providers: dict) -> dict:
        providers = instantiate_all_providers(custom_providers)
        providers = seed_providers(providers)

        try:
            document = generate_fake_document(providers=providers, **custom_lists)
        except AttributeError as e:
            msg = f"Encountered AttributeError when setting up custom_providers and custom_lists. " + \
                  f"It seems that your module might be using custom_lists and custom_providers." + \
                    f"Please ensure you have provided a custom config with custom_providers and custom_lists: {e}"
            raise ConfigError(msg)
        return document

def generate_data_chunk(user_defined_function: callable, chunk_size: int, custom_lists, custom_providers, seed=None):
        """
        Synthetic Data Generator Worker that calls a function that generates a single document.
        The worker will call the function N number of times to generate N docs of data before returning results

        :param user_defined_function: This is the callable that the user defined in their module.
            The callable should be named 'generate_fake_document()'
        :param chunk_size: The number of documents the worker needs to generate before returning them in a list
        :param custom_lists (optional): These are custom lists that the user_defined_function uses to generate random values
        :param custom_providers (optional): These are custom providers (written in Mimesis or Faker) that generate data in a specific way.
            Users define this in the same file as generate_fake_document() function. Custom providers must extend from the BaseProviders class.

        :returns List of documents to be written to disk
        """
        providers = instantiate_all_providers(custom_providers)
        seeded_providers = seed_providers(providers, seed)

        return [user_defined_function(providers=seeded_providers, **custom_lists) for _ in range(chunk_size)]

def generate_dataset_with_user_module(client, sdg_config, user_module, user_config):
        """
        This is used whenever a user has provided their own custom module to generate fake data with.
        This module must contain a function called generate_fake_document(), which houses the definitions of a single synthetic document. It can also
        contain a custom data generators or data providers. It's recommended that custom data generators or data providers are written in Mimesis but they
        can also be written in Faker or use other Python libraries. For best performance, libraries other than Mimesis or Faker should be highly-performant libraries.
        For example, if we want to have a custom data provider that generates a list of random values, we should leverage random library as it bypasses python's GIL.
        For some business use-cases, it might be hard to find highly-performant libraries so writing any code to generate logic is fine but understand that there might be performance limitations

        param: client: Dask client that performs multiprocessing and creates dashboard to visualize task streams
        param: sdg_config: SyntheticDataGenerationConfig instance that houses information related to data corpora to generate
        param: user_module: Python module that user supplies containing logic to generate synthetic documents
        param: user_config: Optional config that specifies custom lists and custom data providers that the custom module uses to generate data.
            This also contains configuration details related to how data is generated (i.e. number of workers to use, max file size in GB, and number of documents in a chunk)

        returns: Does not return results but writes documents to output path
        """
        logger = logging.getLogger(__name__)

        # Fetch settings and custom module components from config that user provided
        generation_settings = get_generation_settings(user_config)
        custom_module_components = user_config.get('CustomSyntheticDataGenerator', {})

        try:
            custom_lists = custom_module_components.get('custom_lists', {})
            custom_providers = {name: getattr(user_module, name) for name in custom_module_components.get('custom_providers', [])}
        except TypeError as e:
            msg = f"Custom config has custom_lists and custom_providers pointing to null values. Either populate or remove."
            raise ConfigError(msg)

        max_file_size_bytes = generation_settings.get('max_file_size_gb') * 1024 * 1024 * 1024
        total_size_bytes = sdg_config.total_size_gb * 1024 * 1024 * 1024
        chunk_size = generation_settings.get('chunk_size')

        generate_fake_document = user_module.generate_fake_document
        avg_document_size = get_avg_document_size(generate_fake_document, custom_providers, custom_lists)

        current_size = 0
        docs_written = 0
        file_counter = 0

        generated_dataset_details = []

        logger.info("Average document size: %s", avg_document_size)
        logger.info("Chunk size: %s docs", chunk_size)
        logger.info("Total GB to generate: %s", sdg_config.total_size_gb)
        logger.info("Max file size in GB: %s", generation_settings.get('max_file_size_gb'))

        console.println(f"Total GB to generate: {sdg_config.total_size_gb}\n"
                        f"Average document size: {avg_document_size}\n"
                        f"Max file size in GB: {generation_settings.get('max_file_size_gb')}\n")

        start_time = time.time()
        with tqdm(total=total_size_bytes,
                  unit='B',
                  unit_scale=True,
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as progress_bar:

            setup_custom_tqdm_formatting(progress_bar)
            while current_size < total_size_bytes:
                file_path = os.path.join(sdg_config.output_path, f"{sdg_config.index_name}_{file_counter}.json")
                file_size = 0
                docs_written = 0

                while file_size < max_file_size_bytes:
                    generation_start_time = time.time()
                    seeds = generate_seeds_for_workers(regenerate=True)
                    logger.info("Using seeds: %s", seeds)

                    futures = [client.submit(generate_data_chunk, generate_fake_document, chunk_size, custom_lists, custom_providers, seed) for seed in seeds]
                    results = client.gather(futures) # if using AS_COMPLETED remove this line

                    writing_start_time = time.time()
                    for data in results:
                        written = write_chunk(data, file_path)
                        docs_written += written
                        written_size = written * avg_document_size
                        current_size += written_size
                        progress_bar.update(written_size)

                    writing_end_time = time.time()

                    file_size = os.path.getsize(file_path)
                    # If it exceeds the max file size, then append this to keep track of record
                    if file_size >= max_file_size_bytes:
                        file_name = file_path.split("/")[-1]
                        generated_dataset_details.append({
                            "file_name": file_name,
                            "docs": docs_written,
                            "file_size_bytes": file_size
                        })

                    generating_took_time = writing_start_time - generation_start_time
                    writing_took_time = writing_end_time - writing_start_time
                    logger.info("Generating took [%s] seconds", generating_took_time)
                    logger.info("Writing took [%s] seconds", writing_took_time)

                    if current_size >= total_size_bytes:
                        file_name = file_path.split("/")[-1]
                        generated_dataset_details.append({
                            "file_name": file_name,
                            "docs": docs_written,
                            "file_size_bytes": file_size
                        })
                        break

                file_counter += 1

            end_time = time.time()
            total_time_to_generate_dataset = round(end_time - start_time)
            progress_bar.update(total_size_bytes - progress_bar.n)

            dataset_size = current_size
            logger.info("Generated dataset in %s seconds. Dataset generation details: %s", total_time_to_generate_dataset, generated_dataset_details)

            return total_time_to_generate_dataset, generated_dataset_details